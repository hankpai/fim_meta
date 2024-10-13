# original author(s):   henry pai (nwrfc)
# contact info:         henry <dot> pai <at> noaa <dot> gov
# last edit by:         hp
# last edit time:       Sep 2024
# last edit comment:    exception handling for getting USGS streamstats

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
import pdb

# ===== global/user vars (not path related)
# common AEP's of interest, leaving as strings to avoid potential rounding errors in array intersections
aep_li = ['0.2', '1', '2', '4', '10', '20', '50']
calc_nwm = False

# ===== debugging var
start_index = 0
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
catfim_meta_fn_suffix = '_catFim_meta.csv'

# output files
log_fn = 'usgs_streamstats.log'
out_fn_prefix = pd.Timestamp.now().strftime('%Y%m%d') + '_'
full_usgs_fn_suffix = '_usgs_all_streamstats.csv'
slim_usgs_fn_suffix = '_usgs_slim_streamstats.csv'

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
    # taking preferred USGS AEP, note yearsofRecord only taken from empirical AEP (vs. regression/algorithmic AEP)
    # otherwise yearsofRecord should be NA, removed for now 
    pref_df = temp_df[temp_df['isPreferred']==True][['value', 'citationID', 'regressionType']]

    if pref_df.empty:
        # handling case when there is no preference
        pref_df = temp_df[['value', 'citationID', 'regressionType']]

    stats_meta = pd.DataFrame(list(pref_df['regressionType']))

    # removes AEP, then splits by PK, then replaces underscore with decimal
    aep_percent = stats_meta['code'].str.rstrip('AEP')\
                                    .str.split('PK', expand=True)[1]\
                                    .str.replace('_', '.')

    row_idxs = np.nonzero(np.in1d(aep_percent, aep_li))[0].tolist()  # getting row indices from aep percent to then pluck from perf_df

    org_df = pref_df.iloc[row_idxs][['value', 'citationID']].reset_index(drop=True)
    org_df['aep_percent'] = aep_percent[row_idxs].reset_index(drop=True)
    org_df['usgs_name'] = stats_meta.iloc[row_idxs]['name'].reset_index(drop=True)
    org_df['usgs_description'] = stats_meta.iloc[row_idxs]['description'].reset_index(drop=True)
    org_df = org_df[org_df['usgs_description'].notna()]

    if org_df.empty:
        # case where json is present but no AEP stats (lilc2, usgs: 09260000)
        return_df = org_df
        logging.info(ahps_lid + ' has a json, but no peak stats')
    else:
        # if there are many preferred, choose weighted (email 2024 Mar).  else choose empirical
        if len(org_df.index) > len(aep_li):
            test_pref_df = org_df[org_df['usgs_description'].str.contains("Weighted")]
            usgs_stat_type = 'weighted' 
            logging.info(ahps_lid + ' : no preferred usgs stats, choose weighted')
            if test_pref_df.empty == True:
                test_pref_df = org_df[org_df['usgs_description'].str.contains("Maximum")]
                logging.info(ahps_lid + ' : no preferred usgs stats, choose empirical')
                usgs_stat_type = 'empirical' 
            
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
            first_word_desc = org_df.usgs_description.str.split().str.get(0)
            most_frequent_word = first_word_desc.mode()[0]  #most frequent

            most_frequent_df = org_df[first_word_desc == most_frequent_word]
            if len(org_df) != len(most_frequent_df):
                logging.info(ahps_lid + ' has multiple flows per percent, most frequent method chosen')
            
            if most_frequent_word == 'Weighted':
                usgs_stat_type = 'weighted'
            elif most_frequent_word == 'Maximum':
                usgs_stat_type = 'empirical'
            elif most_frequent_word == 'Regression':
                usgs_stat_type = 'regression'
            else: 
                usgs_stat_type = 'other'
            assign_pref_df = most_frequent_df

        # some cleaning and sorting
        numeric_aeps = [float(i) for i in assign_pref_df['aep_percent']]
        temp_df = assign_pref_df.copy()
        temp_df['aep_percent'] = numeric_aeps
        rename_df = temp_df.rename(columns={'value':'usgsFlow_cfs'})
        sort_df = rename_df.sort_values('usgsFlow_cfs')
        return_df = sort_df.drop(['usgs_description'], axis=1)
        return_df['usgs_stat_type'] = usgs_stat_type

    return(return_df)

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
    
def get_site_info(mapping_df, request_header, aoi, ds):
    """
    loop through getting usgs streamstats and attempted NWM retrospective v3 streamstats
    """
    loop_li = []
    
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
                usgs_df = org_usgs(usgs_json, row.ahps_lid)

                if usgs_df.empty == False:
                    if calc_nwm:
                        # as of 2024 Sep, the retro run goes from 1979 Feb to 2023 Feb
                        nwm_ds = ds.sel(feature_id=row.nwm_seg)['streamflow'].sel(time=slice('1979-10-01', '2022-09-30'))
                        water_yr = (nwm_ds.time.dt.month >=10) + nwm_ds.time.dt.year
                        nwm_ds.coords['water_yr'] = water_yr # https://stackoverflow.com/questions/72268056/python-adding-a-water-year-time-variable-in-an-x-array
                        nwm_df = org_nwm(nwm_ds, water_yr)

                        site_df = nwm_df.merge(usgs_df, how='left', on='aep_percent')
                    else:
                        site_df = usgs_df
                                        
                    site_df.insert(0, 'ratingMax_cfs', row.rating_max_flow)
                    site_df.insert(0, 'nwm_streamOrder', row.nwm_feature_data_stream_order)
                    site_df.insert(0, 'rfc_headwater', row.rfc_headwater)
                    site_df.insert(0, 'wfo', row.nws_data_wfo)
                    site_df.insert(0, 'ahps_lid', row.ahps_lid)

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

                    print(str(i) + ' : ' + aoi + ' - ' + row.ahps_lid + ' = ' + str(usgs_num_str))
                    logging.info(str(i) + ' : ' + aoi + ' - ' + row.ahps_lid + ' = ' + str(usgs_num_str))

                    external_count += 1
                    loop_li.append(site_df)

    logging.info('scraping done')
    return_df = pd.concat(loop_li)
    return(return_df)
    
def main():
    with open(os.path.join(ctrl_dir, yaml_fn)) as f:
    # NWRFC settings for request headers, keeping hidden in yaml file
        yaml_data = yaml.full_load(f)
        request_header = {'User-Agent' : yaml_data['user_agent']}
    
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
        files_li = glob.glob(in_dir + '/*_' + aoi + catfim_meta_fn_suffix)
        last_catfim_fullfn = max(files_li, key=os.path.getctime)
        logging.info(aoi + ' is using ' + last_catfim_fullfn + ' for getting stats')
        catfim_df = pd.read_csv(last_catfim_fullfn)

        usgs_map_df = catfim_df[['ahps_lid',
                                 'nwm_seg',
                                 'usgs_gage',
                                 'nws_data_wfo',
                                 'rfc_headwater',
                                 'nwm_feature_data_stream_order',
                                 'rating_max_flow']]
        
        stats_df = get_site_info(usgs_map_df, request_header, aoi, ds)

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
        
        stats_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + full_usgs_fn_suffix), index=False)
        simple_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + slim_usgs_fn_suffix))

        logging.info(aoi + ' streamstats gathering has finished')
    
    logging.shutdown()

if __name__ == '__main__':
    main()
