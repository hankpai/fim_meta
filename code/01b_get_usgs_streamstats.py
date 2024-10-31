# original author(s):   henry pai (nwrfc)
# contact info:         henry <dot> pai <at> noaa <dot> gov
# last edit by:         hp
# last edit time:       Oct 2024
# last edit comment:    edited to return all USGS streamstats, and chooses non-regulatory aep's if available

# summary:
# aggregates AEP stats from USGS; note NWM stats from USGS Bulletin 17C eq. 11 - implemented, but not output as values were >> usgs stats

# things to be careful about:
# - NWM and AEP calc may have different methods (local regression vs the equation listed in summary)

# inputs:
# - usgs streamstats, example call (from nws gage analysis): https://streamstats.usgs.gov/gagestatsservices/statistics?statisticGroups=pfs&stationIDOrCode=
#   - use USGS's dataretrieval?: https://doi-usgs.github.io/dataretrieval-python/
# - nwm AEP stats (not yet implemented, values started straying wildly - given time it took, didn't seem worth it)
#   - resource for extracting zarr data (hopefully same for NWM v3.0): https://www.hydroshare.org/resource/c4c9f0950c7a42d298ca25e4f6ba5542/
#   - other resource for v3: https://www.hydroshare.org/resource/6ca065138d764339baf3514ba2f2d72f/
#   - nwm retrospective: https://registry.opendata.aws/nwm-archive/
#   - nwm variables: https://github.com/NOAA-Big-Data-Program/bdp-data-docs/blob/main/nwm/README.md

# TODO
# [ ] look at xarray group by, can this be done by coordinate?
# [ ] perhaps use USGS dataretrieval package, now in also in python!
# [ ] need stats on annual peaks?
# [ ] filter for low outliers? https://code.usgs.gov/water/stats/MGBT/-/tree/master?ref_type=heads

import os
import yaml
import pathlib
import glob
import pandas as pd
import numpy as np
import scipy
import json
import logging
import urllib3
import xarray as xr
import fsspec
import re
import pdb

# ===== global/user vars (not path related)
# common AEP's of interest, leaving as strings to avoid potential rounding errors in array intersections
aep_li = ['0.2', '1', '2', '4', '10', '20', '50']
calc_nwm = False

# ===== debugging var
start_index = 0 # 285 crli2 for CR, good test for regulated, multiple aep methods
#start_index = 398 # should be used when debugging, otherwise comment out

# ===== directories & filenames (site/computer specific)
work_dir = pathlib.Path(__file__).parent.parent  # IDE independent

ctrl_dir = os.path.join(work_dir, "ctrl")   # csv files controlling columns and wfo's to scrape
in_dir = os.path.join(work_dir, 'out', 'catfim')
log_dir = os.path.join(work_dir, "logs")
out_dir = os.path.join(work_dir, "out", "stats")

# yaml file
yaml_fn = 'config.yaml'

# contorl file indicating wfos/rfcs to scrape
areas_fn = 'nws_aois.csv'

# input file info
catfim_meta_fn_suffix1 = '_catFim_meta.csv'

# output files
log_fn = 'usgs_streamstats.log'
out_fn_prefix = pd.Timestamp.now().strftime('%Y%m%d') + '_'
full_usgs_fn_suffix1 = '_usgsAllStats.csv'
slim_usgs_fn_suffix1 = '_usgsSlimStats.csv'

with open(os.path.join(ctrl_dir, yaml_fn)) as f:
# NWRFC settings for request headers, keeping hidden in yaml file
# not super happy to make this global
    yaml_data = yaml.full_load(f)
    request_header = {'User-Agent' : yaml_data['user_agent']}
    catfim_meta_fn_suffix2 = '_' + yaml_data['station_src'] + 'Stalist' + catfim_meta_fn_suffix1
    full_usgs_fn_suffix2 =  '_' + yaml_data['station_src'] + 'Stalist' + full_usgs_fn_suffix1
    slim_usgs_fn_suffix2 = '_' + yaml_data['station_src'] + 'Stalist' + slim_usgs_fn_suffix1

# ===== url info
usgs_url_prefix = 'https://streamstats.usgs.gov/gagestatsservices/statistics?statisticGroups=pfs&stationIDOrCode='
nwm_retro_bucket_url = 's3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr'

# ===== initial set up for requests and logging
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(message)s',
                    filename=os.path.join(log_dir, log_fn),
                    filemode='w',
                    #level=logging.DEBUG,
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

# ===== functions
def org_usgs(usgs_json, ahps_lid):
    """
    pulls in relevant usgs streamstats for thresholds listed in the global var aep_li
    """
    temp_df = pd.DataFrame(usgs_json)
    
    # changing some of the order here, would like to take all the USGS AEP's, preferred or not, then do the temporary data frame
    temp_select_cols_df = pd.concat([temp_df['isPreferred'],
                                     temp_df[['value', 'citationID']],
                                     pd.json_normalize(temp_df['regressionType'])], axis=1)
    
    # pulling AEP rows
    aep_all_df = temp_select_cols_df[temp_select_cols_df['code'].str.contains('AEP')]\
                                                                .drop(['id', 
                                                                       'metricUnitTypeID',
                                                                       'englishUnitTypeID',
                                                                       'statisticGroupTypeID',
                                                                       'description'], axis=1)\
                                                                .reset_index(drop=True)

    if aep_all_df.empty:
        # case where json is present but no AEP stats (lilc2, usgs: 09260000)
        final_pref_df = aep_all_df.copy()
        usgs_aeps_df = aep_all_df.copy()
        logging.info(ahps_lid + ' has a json, but no AEP stats')
    else:
        # pulling AEP numeric values
        usgs_aeps = aep_all_df['code'].str.rstrip('AEP')\
                                      .str.split('PK', expand=True)[1]\
                                      .str.replace('_', '.')

        usgs_row_idxs = np.nonzero(np.in1d(usgs_aeps, aep_li))[0].tolist()  # getting row indices from aep percent to then pluck from perf_df

        usgs_aeps_df = aep_all_df.copy().iloc[usgs_row_idxs].reset_index(drop=True)
        usgs_aeps_df['aep_percent'] = [float(i) for i in usgs_aeps[usgs_row_idxs].reset_index(drop=True)]

        usgs_aeps_df.rename(columns={'value' : 'usgsFlow_cfs',
                                     'isPreferred' : 'usgs_pref',
                                     'name' : 'usgs_name'}, inplace=True)
                                     #'description' : 'usgs_description'}, inplace=True)

        # second answer: https://stackoverflow.com/questions/9987483/elif-in-list-comprehension-conditionals
        # mapping stat type to first word of description
        stat_dict = {'WPK' : 'weighted', 'PK' : 'station', 'RPK' : 'regression', 'APK' : 'alternate', 'GPK' : 'regulated'}
        nws_pref_dict = {'WPK' : 1, 'PK' : 2, 'RPK' : 3, 'APK' : 4, 'GPK' : 10}
        usgs_aeps_df['usgs_stat_type'] = [stat_dict.get(re.sub(r'\d+', ' ', code).split(' ')[0], 'other') for code in usgs_aeps_df['code']]
        usgs_aeps_df['nws_pref_order'] = [nws_pref_dict.get(re.sub(r'\d+', ' ', code).split(' ')[0], ) for code in usgs_aeps_df['code']]
        usgs_aeps_df.loc[usgs_aeps_df['usgs_stat_type'] == 'regulated', 'usgs_pref'] = False  # regulated should be used as last result, want naturalized flow
        pref_df = usgs_aeps_df[usgs_aeps_df['usgs_pref'] == True]

        if pref_df.empty:
            logging.info(ahps_lid + ' : no preferred usgs stats, choose by nws_pref_order: weighted, station, regression, alternate, other, regulated')

            if len(usgs_aeps_df.index) > len(aep_li):
                test_pref_df = usgs_aeps_df.copy().loc[usgs_aeps_df['nws_pref_order'] == usgs_aeps_df.nws_pref_order.min()]
                if len(test_pref_df) > len(aep_li):
                    # needed for mhpp1 in marfc
                    most_frequent_cite = test_pref_df.citationID.mode()[0]
                    final_pref_df = test_pref_df[test_pref_df.citationID == most_frequent_cite].sort_values('usgsFlow_cfs')
                    logging.info(ahps_lid + ' has multiple flows per percent, most frequent citation chosen')
            else:
                final_pref_df = usgs_aeps_df.copy().sort_values('usgsFlow_cfs')
            logging.info(ahps_lid + ' has a no usgs preferred designation')
        else:
            # if there are many preferred, choose weighted (email 2024 Mar).  else choose empirical
            if len(pref_df.index) > len(aep_li):
                test_pref_df = pref_df.loc[pref_df['nws_pref_order'] == pref_df.nws_pref_order.min()]
                logging.info(ahps_lid + ' : many preferred usgs stats, choose by nws_pref_order: weighted, station, regression, alternate, other, regulated')
                
                # if the preferred has old citations, choose the most frequent citation (ensures one flow per percent)
                # coss2 (usgs: 06482610) is an example
                if len(test_pref_df) > len(aep_li):
                    most_frequent_cite = test_pref_df.citationID.mode()[0]
                    assign_pref_df = test_pref_df[test_pref_df.citationID == most_frequent_cite]
                    logging.info(ahps_lid + ' has multiple flows per percent, most frequent citation chosen')
                else:
                    assign_pref_df = test_pref_df
                
            else:
                # so, some exception handling as aftw3 (usgs: 05430500) has two methods that are 'preferred'
                # so handling the by choosing the most 'frequent' preferred method
                code_desc = [re.sub(r'\d+', ' ', code).split(' ')[0] for code in pref_df['code']]

                most_frequent_code = pd.Series(code_desc).mode()[0]  #most frequent

                most_frequent_df = pref_df.iloc[[i for i, desc in enumerate(code_desc) if desc == most_frequent_code]]
                if len(pref_df) != len(most_frequent_df):
                    logging.info(ahps_lid + ' has multiple flows per percent, most frequent method chosen')
                
                assign_pref_df = most_frequent_df

            # sorting
            final_pref_df = assign_pref_df.copy().sort_values('usgsFlow_cfs')

    # inserting nws/my preference 
    same_row_ids = pd.merge(usgs_aeps_df.reset_index(), final_pref_df, on=final_pref_df.columns.tolist())['index'].tolist()
    usgs_aeps_df.insert(0, 'nws_pref', False)
    usgs_aeps_df.loc[same_row_ids, ['nws_pref']] = True

    return(final_pref_df, usgs_aeps_df)

def org_nwm(nwm_ds, water_yr):
    """
    solves for Eq 11 for USGS Bulletin 17C, Chapter 5 of Book 4 for AEP estimates.  This should be similar to WRDS estimates, but does NOT perform
    low-outlier tests

    quote from WRDS site: https://vlab.noaa.gov/redmine/projects/wrds/wiki/WRDS_Location_API

    'Streamflow Annual Exceedance Probabilities (AEP) were calculated for 2, 5, 10, 25, 50, and 100 year return periods. The streamflow AEPs were
    estimated using methods outlined in the USGS Bulletin 17C. Annual peak flows were derived from systematic modelled output from the NWM v2.1
    41-year retrospective and these were then used with the "Parameter Estimation—Simple Case" (USGS Bulletin 17C, page 25) with MGBT
    (Multiple Grubbs-Beck Low-Outlier Test) to identify and remove PILF (Potentially Influential Low Flows), and a frequency factor (k) which is a
    function of the skew coefficient (G).'
    """
    m3_to_f3 = 100**3 / (2.54**3) / (12**2)
    yr_pks = nwm_ds.groupby(water_yr).max().values # time consuming step & heavy download; also groupby coordinate?
    mean_pks = yr_pks.mean()
    std_pks = np.std(yr_pks)
    skew_pks = scipy.stats.skew(yr_pks)

    alpha = 4 / (skew_pks**2)
    beta = np.sign(skew_pks) * (((std_pks**2) / alpha)**0.5)
    tau = mean_pks - alpha * beta

    q_li = [1 - float(aep_str)/100 for aep_str in aep_li]
    x_q_li = [round((tau + beta * (scipy.special.gammaincinv(alpha, q))) * m3_to_f3) for q in q_li]

    return_df = pd.DataFrame()
    return_df['aep_percent'] = aep_li
    return_df['nwmFlow_cfs'] = x_q_li

    return(return_df)

def insert_site_meta(df, row):
    ''' just adding to start of dataframe'''
    df.insert(0, 'ratingMax_cfs', row.rating_max_flow)
    df.insert(0, 'nwm_streamOrder', row.nwm_feature_data_stream_order)
    df.insert(0, 'rfc_headwater', row.rfc_headwater)
    df.insert(0, 'wfo', row.nws_data_wfo)
    df.insert(0, 'ahps_lid', row.ahps_lid)
    
def get_site_info(mapping_df, aoi, ds):
    """
    loop through getting usgs streamstats and attempted NWM retrospective v3 streamstats
    """
    pref_li = []
    all_li = []
    
    external_count = 0
    for i, row in mapping_df.iloc[start_index:].iterrows():
        if row.usgs_gage != 0:  # this line is kept to make sure debugging is easier iterating via catfim metadata file
            
            usgs_num_str = str(row.usgs_gage).zfill(8)

            if len(usgs_num_str) != 8:
                logging.info(row.ahps_lid + ' has wrong number of digts')
                        
            http = urllib3.PoolManager()
            usgs_url = usgs_url_prefix + usgs_num_str
            usgs_response = http.request('GET', usgs_url, headers=request_header)
            usgs_json = json.loads(usgs_response.data.decode('utf8'))

            if len(usgs_json) == 0:
                logging.info(row.ahps_lid + ' missing usgs json or empty page')
            else:
                print(str(i) + ' : ' + aoi + ' - ' + row.ahps_lid + ' = ' + str(usgs_num_str))
                pref_df, all_df = org_usgs(usgs_json, row.ahps_lid)

                if pref_df.empty == False:
                    if calc_nwm:
                        # as of 2024 Sep, the retro run goes from 1979 Feb to 2023 Feb
                        nwm_ds = ds.sel(feature_id=row.nwm_seg)['streamflow'].sel(time=slice('1979-10-01', '2022-09-30'))
                        water_yr = (nwm_ds.time.dt.month >=10) + nwm_ds.time.dt.year
                        nwm_ds.coords['water_yr'] = water_yr # https://stackoverflow.com/questions/72268056/python-adding-a-water-year-time-variable-in-an-x-array
                        nwm_df = org_nwm(nwm_ds, water_yr)

                        site_df = nwm_df.merge(pref_df, how='left', on='aep_percent')
                    else:
                        site_df = pref_df
                    
                    insert_site_meta(site_df, row)
                    insert_site_meta(all_df, row)

                    """ below is commented out to write to file one time in main section.  ensures simple_df is written correctly via column
                    simple_df = site_df[['ahps_lid', 
                                        'wfo', 
                                        'rfc_headwater',
                                        'usgs_stat_type', 
                                        'ratingMax_cfs', 
                                        'usgsFlow_cfs', 
                                        'aep_percent']].pivot(index=['ahps_lid', 'wfo', 'rfc_headwater', 'usgs_stat_type', 'ratingMax_cfs'],
                                                            columns='aep_percent', 
                                                            values='usgsFlow_cfs') 
                    
                    if external_count == 0 and start_index == 0:
                        site_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + full_usgs_fn_suffix), index=False)
                        simple_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + slim_usgs_fn_suffix))
                    else:
                        site_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + full_usgs_fn_suffix), index=False, mode='a', header=False)
                        simple_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + slim_usgs_fn_suffix), mode='a', header=False)
                    """

                    logging.info(str(i) + ' : ' + aoi + ' - ' + row.ahps_lid + ' = ' + str(usgs_num_str))

                    external_count += 1
                    pref_li.append(site_df)
                    all_li.append(all_df)

    logging.info('scraping done')
    return_pref_df = pd.concat(pref_li)
    return_all_df = pd.concat(all_li)

    return(return_pref_df, return_all_df)
    
def main():
    areas_df = pd.read_csv(os.path.join(ctrl_dir, areas_fn))
    aois_li = areas_df.loc[areas_df['include'] == 'x']['area'].tolist()
    
    if calc_nwm:
        logging.info('loading begun for NWM retro bucket')
        ds = xr.open_zarr(fsspec.get_mapper(nwm_retro_bucket_url, anon=True),consolidated=True)
        logging.info('loading complete for NWM retro bucket')
    else:
        ds = None

    for aoi in aois_li:
        logging.info(aoi + ' streamstats gathering has started')
        files_li = glob.glob(in_dir + '/*_' + aoi + catfim_meta_fn_suffix2)
        last_catfim_fullfn = max(files_li, key=os.path.getctime)
        logging.info(aoi + ' is using ' + last_catfim_fullfn + ' for getting stats')
        catfim_df = pd.read_csv(last_catfim_fullfn)
        
        usgs_map_df = catfim_df[['ahps_lid',
                                 'nwm_seg',
                                 'usgs_gage',
                                 'nws_data_wfo',
                                 'rfc_headwater',
                                 'nwm_feature_data_stream_order',
                                 'rating_max_flow']].drop_duplicates().reset_index(drop=True)

        if len(catfim_df) > len(usgs_map_df):
            # alaska/hawaii has some duplicate rows for nlih1
            print('there are duplicate rows from the catfim meta file')
            logging.info(aoi + ' has duplicate rows in ' + last_catfim_fullfn)
        
        stats_df, all_df = get_site_info(usgs_map_df, aoi, ds)

        all_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + full_usgs_fn_suffix2), index=False)

        simple_df = stats_df[['ahps_lid', 
                              'wfo', 
                              'rfc_headwater',
                              'nwm_streamOrder',
                              'usgs_stat_type', 
                              'ratingMax_cfs', 
                              'usgsFlow_cfs', 
                              'aep_percent']].pivot(index=['ahps_lid', 'wfo', 'rfc_headwater', 'nwm_streamOrder', 'usgs_stat_type', 'ratingMax_cfs'],
                                                    columns='aep_percent', 
                                                    values='usgsFlow_cfs') 
        
        simple_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + slim_usgs_fn_suffix2))

        logging.info(aoi + ' streamstats gathering has finished')
    
    logging.shutdown()

if __name__ == '__main__':
    main()
