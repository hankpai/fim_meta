# original author(s):   henry pai (nwrfc)
# contributors:         Benjamin Sipprell - usgs and ahps fim info
# contact info:         henry <dot> pai <at> noaa <dot> gov
# last edit by:         hp
# last edit time:       Sep 2024
# last edit comment:    added various capabilities to scale beyond one rfc, determination of thresholds by observed primary unit, including ahps and usgs fim meta, fema study date info, and option to not redownload DEM & FEMA dates

# summary:
# aggregates various location and impact/thresholds metadata to both automate and enhance some data entry for
# static fim reviews

# things to be careful about:
# - DEM meta is directly from the national map/USGS query service, not necessarily when HAND was derived (if new tiles are in, DEM meta may be more recent than HAND)
# - Threshold category (stage vs. flow) was determined via code.  No explicit metadata label in NWPS api call.  WFO's should be able to quickly verify

# inputs:
# - json files for site information with stage and flow static site info.  These files downloaded locally
#   from logged in database queries (login required):
#   > https://maps.water.noaa.gov/server/rest/services/fim_libs/static_stage_based_catfim_noaa/MapServer/0/query
#   > https://maps.water.noaa.gov/server/rest/services/fim_libs/static_flow_based_catfim_noaa/MapServer/0/query
# - json file for ahps fim maps
# - json file for usgs fim maps
# - scraper nwps/nrldb api (threshold info): https://api.water.noaa.gov/nwps/v1/gauges/<lid>
# - scraper nwps/nrldb api (rating info): https://api.water.noaa.gov/nwps/v1/gauges/<lid>/ratings
# - nwps/nrldb api info: https://api.water.noaa.gov/nwps/v1/docs/
# - dem api (asking about resolution field): https://epqs.nationalmap.gov/v1/docs, https://apps.nationalmap.gov/epqs/
# - fema hazard layer: https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer
# - nwm streamstats (login/cac/secure OWP comp required?): https://vlab.noaa.gov/redmine/projects/wrds/wiki/WRDS_Location_API

# FIM Reviewer fields, just in case we need to join geodatabases. For point reviews,
# check marks for fields that CAN be missing for static FIM (polygon fields denoted
# in curly brackets to the right of the field below):
# [ ] Review Status
# [ ] FIM Mode
# [ ] Fim Type
# [ ] FIM Version
# [x] Reference Time (empty)
# [x] Issue Time (empty)
# [ ] FIM Status
# [x] FIM Behavior
# [x] Max HAND Stage (ft)
# [x] Max Streamflow (cfs)
# [ ] Nearest AHPS Gauge
# [x] Max Flood Status
# [x] Flood Impact Assessment
# [x] RFC QPF Duration (hr)
# [ ] Review Comments
# [x] Development Response
# [x] Linked Content
# [ ] Remote Sensing Analysis   {poly}
# [ ] Creation Date             {poly}
# [ ] Creator                   {poly}
# [ ] Attachments               {poly, but not bold field in FIM reviewer}

# TODO:
# [x] Fork off partner URL requests (USGS DEM point service, FEMA) as they sometimes error out
# [ ] Automate esri db calls?  Need to pass login info (FIM10 seems possible on non-noaa ESRI server?  Done with FEMA ESRI call, can do a small rewrite when FIM30 is live
# [ ] Better handle warnings from line 220'ish: merging on int and float columns where float values are not equal...
# [ ] Incorporate usgs streamstats (i.e., AEP) where available (usually minimal impact upstream).
#     Example api call: https://streamstats.usgs.gov/gagestatsservices/statistics?statisticGroups=pfs&stationIDOrCode=14191000
# [ ] Incorporate NWM AEP stats (how? ask MARFC) - also WRES
# [ ] Compare NRLDB max stage & flow rating with USGS (look for usgs url/api or just url call)
# [ ] Fill in metadata for FEMA Hazard Layer age at gage
# [ ] Should probably be transformed to OOP if further expanded to above fields... just joining different databases.

import glob
import os
import pathlib
import pandas as pd
import numpy as np
import json
import logging
import urllib
import urllib3
import time
import yaml
import pdb

# ===== global/user vars (not path related)
get_partner = False  # gets usgs DEM and fema hazard info if True

# in NWPS, if both flow and stage are populated, the code takes care of 'most cases' in the function: check_threshold_type
# the site below is still a flow threshold site, but has both flow and stage populated in the api metadata
# only example in NWRFC: https://api.water.noaa.gov/nwps/v1/gauges/eagi1
# Crane Johnson suggested looking at the primary unit?
exception_li = ['eagi1']

# ===== debugging var
start_index = 0
#start_index = 398 # should be used when debugging, otherwise comment out

# ===== directories & filenames (no longer site specific, but direcotry structure specific)
work_dir = pathlib.Path(__file__).parent.parent  # IDE independent

ctrl_dir = os.path.join(work_dir, "ctrl")   # csv files controlling columns and wfo's to scrape
in_dir = os.path.join(work_dir, 'in')
stage_dir = os.path.join(in_dir, "stage")
flow_dir = os.path.join(in_dir, "flow")
nonhand_fim_dir = os.path.join(in_dir, 'nonhand_fim')
log_dir = os.path.join(work_dir, "logs")
out_dir = os.path.join(work_dir, "out", "catfim")

# yaml file
yaml_fn = 'config.yaml'

# contorl file indicating wfos/rfcs to scrape
areas_fn = 'nws_aois.csv'

# control files indicating which columns/fields from json file to keep on
stage_columns_fn = 'stage_fim_column_ids.csv'
flow_columns_fn = 'flow_fim_column_ids.csv'

# input files &/ filename structure
stage_fn_suffix = '_stage_catfim_meta.txt'
flow_fn_suffix = '_flow_catfim_meta.txt'
ahps_fim_fn = 'ahps_fim.json'
usgs_fim_fn = 'usgs_fim.json'

# output files
log_fn = 'catfim_meta.log'
out_fn_prefix = pd.Timestamp.now().strftime('%Y%m%d') + '_'
combined_out_fn_suffix =  '_catFimReview_meta_stagePrioritized_wExceptions.csv'
raw_static_fims_fn_suffix = '_raw_catFim_meta.csv'
org_static_fims_fn_suffix = '_catFim_meta.csv'
nwps_impact_fn_suffix = '_impacts_meta.csv'

# ===== url info
nwps_base_url = 'https://api.water.noaa.gov/nwps/v1/gauges/'
rtgs_post_str = 'ratings'

dem_base_url = 'https://epqs.nationalmap.gov/v1/json?'
dem_base_suffix_url = '&wkid=4326&units=Feet&includeDate=true'

# https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/3/query?&geometry=-113.931%2C46.87722&geometryType=esriGeometryPoint&outFields=*&f=html
fema_base_url = 'https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/3/query?'

# ===== initial set up for requests and logging
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(message)s',
                    filename=os.path.join(log_dir, log_fn),
                    filemode='w',
                    #level=logging.DEBUG,
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

# ===== functions
def convert_fim_json_df(fullfn):
    """
    dealing with fim geodatabase json output (see above links), with some nesting
    returns df with all columns
    """
    with open(fullfn) as json_data:
        j_data = json.load(json_data)
        dict_df = pd.DataFrame(j_data['features'])
        org_df = pd.DataFrame(list(dict_df['attributes'])) # probably has a more pythonic, cleaner way

    return org_df

def clean_stage_df(df):
    """
    main function is to strip unnecessary (imo) metadata in the geodatabase call
    columns that need cleaning:
    - nws_data_map_link -> just split to lat/lon
    - nrldb_timestamp   -> just put "update" & "tz" in column name, only have timestamp in entry
    - nwis_timestamp    -> same as above
    """
    # splitting lat lon
    lat_lon = df['nws_data_map_link'].str.split("loc:").str[1].str.split("+", expand=True)
    
    # stripping some text, think date is fine for this field
    nrldb_date_updated = df['nrldb_timestamp'].str.split('updated: ').str[1].str.split(' ').str[0]
    nwis_date_updated = df['nwis_timestamp'].str.split('updated: ').str[1].str.split(' ').str[0]

    df['lat'] = lat_lon[0]
    df['lon'] = lat_lon[1]
    df['fim_nrldb_update_date'] = nrldb_date_updated
    df['fim_nwis_update_date'] = nwis_date_updated

    return_df = df.drop(['nws_data_map_link', 'nrldb_timestamp', 'nwis_timestamp'], axis=1)

    return return_df

def check_threshold_type(lid, obs_primary_unit, thresholds_df, rating_df, impacts_df):
    """
    returns amended df to thresholds with impacts, rated variable, and adds max rating info
    """
    # -9999 for missing threshold value
    threshold_check = thresholds_df[thresholds_df < 0].count()

    # this check won't necessarily work if there are mixed flow and stage thresholds
    # note, change in logic looking at primary unit in observed data (recommended by Crane Johnson via GChat)
    if obs_primary_unit == '':
        if threshold_check['flow'] > threshold_check['stage']:
            threshold_type = 'stage'
        elif threshold_check['flow'] == threshold_check['stage']:
            # several sites have both stage and flow entered
            if lid in exception_li:
                threshold_type = 'flow'
            else:
                threshold_type = 'stage'
        else:
            threshold_type = 'flow'
    elif obs_primary_unit == 'kcfs':
        threshold_type = 'flow'
    elif obs_primary_unit == 'ft':
        threshold_type = 'stage'

    # filter -9999 values, like missing moderate, major categories
    org_thresholds_df = thresholds_df[thresholds_df[threshold_type] >=0].reset_index().rename(columns={'index':'category'})

    if not org_thresholds_df.empty:
        if threshold_check['flow'] == threshold_check['stage']:
            logging.info(lid + ' has both flow and stage thresholds defined.')

    if impacts_df.empty:
        impacts_df['impact_val'] = None
        impacts_df['statement'] = None
    else:
        impacts_df.rename(columns={impacts_df.columns[0]:'impact_val'}, inplace=True)

    # joins threshold and impacts table, fills in missing stages/flows depending on threshold type
    thresh_imp_df = org_thresholds_df.merge(impacts_df, left_on=threshold_type, right_on='impact_val', how='outer')
    thresh_imp_df['merge_thresholds'] = thresh_imp_df[threshold_type].where(thresh_imp_df[threshold_type].notnull(), thresh_imp_df['impact_val'])
    thresh_imp_df[threshold_type] = thresh_imp_df['merge_thresholds']
        
    org_df = thresh_imp_df.drop(['merge_thresholds', 'impact_val'], axis=1).sort_values(threshold_type)

    if rating_df.empty:
        logging.info('no nws rating for ' + lid)
        max_stg = -9999
        max_flow = -9999
    else:
        rating_max = rating_df.max()
        max_stg = rating_max['stage']
        max_flow = rating_max['flow']
        
        # applies missing threshold variable to rating.  if stage-flow pair doesn't exist, does linear interpolation.  no ratings extensions
        if threshold_type == 'stage':
            org_df['flow'] = np.round(np.interp(org_df['stage'], rating_df['stage'], rating_df['flow'], left=-9999, right=-9999), 0)
        else:
            org_df['stage'] = np.round(np.interp(org_df['flow'], rating_df['flow'], rating_df['stage'], left=-9999, right=-9999), 2)

    return_df = org_df

    return threshold_type, max_stg, max_flow, return_df

def add_meta_cols(df, threshold_type, max_stg, max_flow, dem_resolution, dem_yr, ahps_fim_exist, usgs_fim_exist, usgs_fim_yr, fema_effective_date, rfc_headwater):
    """
    tacking on extra metadata columns, ones with single values
    """
    df['threshold_type'] = threshold_type
    df['rfc_headwater'] = rfc_headwater
    df['rating_max_stage'] = max_stg
    df['rating_max_flow'] = max_flow
    df['ahps_fim'] = ahps_fim_exist
    df['usgs_fim'] = usgs_fim_exist
    df['usgs_fim_yr'] = usgs_fim_yr
    df['fema_eff_date'] = fema_effective_date
    df['dem_yr'] = dem_yr
    df['dem_resolution'] = dem_resolution

    return df

def get_site_info(fims_df, aoi, request_header):
    """
    Gets site information that can be downloaded/obtained in bulk & needs to be looped at a per site level
    exeptions handled:
    - missing ratings, impacts, metadata/gage page
    returns amended df to thresholds with impacts, rated variable, and adds max rating & headwater info
    """
    loop_li = [] # big loop to combine impacts into multiple rows per lid
    site_li = [] # organized site info

    df = fims_df.reset_index(drop=True)

    # loading nonhand fim info
    with open(os.path.join(nonhand_fim_dir, ahps_fim_fn)) as ahps_fim_f:
        ahps_fim_json = json.load(ahps_fim_f)
    
    with open(os.path.join(nonhand_fim_dir, usgs_fim_fn)) as usgs_fim_f:
        usgs_fim_json = json.load(usgs_fim_f)

    ahps_fim_df = pd.DataFrame(ahps_fim_json['features'])
    ahps_fim_df['ahps_lid'] = ahps_fim_df['ahps_lid'].str.lower()

    usgs_fim_df = pd.json_normalize(usgs_fim_json['features'])

    if get_partner == False:
        files_li = glob.glob(out_dir + '/*_' + aoi + org_static_fims_fn_suffix)
        last_partner_fullfn = max(files_li, key=os.path.getctime)
        partner_df = pd.read_csv(last_partner_fullfn)
        logging.info('site scraping for nwps only, nationalmaps and fema data pulled from: ' + os.path.split(last_partner_fullfn)[1])
    else:
        logging.info('site scraping (nwps, nationalmaps, fema) begins')
    
    external_count = 0
    for i, row in df.iloc[start_index:].iterrows():
        lid = row['ahps_lid']
        gage_url = nwps_base_url + lid
        rating_url = nwps_base_url + lid + "/" + rtgs_post_str
        lon = row['lon']
        lat = row['lat']
        # example dem url: https://epqs.nationalmap.gov/v1/json?x=-122.59&y=45.53&wkid=4326&units=Feet&includeDate=true
        dem_url = dem_base_url + 'x=' + lon + '&y=' + lat + dem_base_suffix_url

        http = urllib3.PoolManager()

        # getting rating info for max stage & flow
        rating_response = http.request('GET', rating_url, headers = request_header)
        rating_json = json.loads(rating_response.data.decode('utf8'))
        rating_df = pd.DataFrame(rating_json['data'])

        # getting thresholds and impacts info
        gage_response = http.request('GET', gage_url, headers = request_header)
        gage_json = json.loads(gage_response.data.decode('utf8'))

        if get_partner: 
            # getting dem info - resolution (though unclear what this value represents) and aquisition date
            #pdb.set_trace()
            dem_response = http.request('GET', dem_url, headers=request_header)
            dem_json = json.loads(dem_response.data.decode('utf8'))

            # getting fema info
            # some help: https://gis.stackexchange.com/questions/427434/query-feature-service-on-esri-arcgis-rest-api-with-python-requests
            fema_params = {
                'geometry': str(lon) + ',' + str(lat),
                'geometryType': 'esriGeometryPoint',
                'returnGeometry': 'false',
                'outFields': '*',
                'f': 'pjson'
            }
            fema_url = fema_base_url + urllib.parse.urlencode(fema_params)
            fema_response = http.request('GET', fema_url, headers = request_header)
            fema_json = json.loads(fema_response.data.decode('utf8'))

        # checking if metadata exists
        if gage_response.status == 200:
            thresholds_df = pd.DataFrame(gage_json['flood']['categories']).transpose()
            impacts_df = pd.DataFrame(gage_json['flood']['impacts'])
            status_df = pd.DataFrame(gage_json['status'])

            # note, this assumes nwps posts observed (think so for all sites? not sure) and forecasts (seems like rfc's don't necessarily send?).  will stick with observed
            obs_primary_unit = status_df['observed']['primaryUnit']

            # building thresholds & impacts info into expanded table with more roows
            threshold_type, max_stg, max_flow, thresh_imp_df = check_threshold_type(lid, obs_primary_unit, thresholds_df, rating_df, impacts_df)

            # from Benjamin, metadata for partner fims
            if ahps_fim_df[ahps_fim_df['ahps_lid'] == lid].empty:
                ahps_fim_exist = 'no'
            else:
                ahps_fim_exist = 'yes'

            if usgs_fim_df[usgs_fim_df['attributes.AHPS_ID'] == lid].empty:
                usgs_fim_exist = 'no'
                usgs_fim_yr = ''
            else:
                usgs_fim_exist = 'yes'
                usgs_fim_yr = usgs_fim_df[usgs_fim_df['attributes.AHPS_ID'] == lid]['attributes.STUDY_DATE'].values[0]

            # other relvant metadata from NWS sourced scraping
            if gage_json['upstreamLid'] == "":
                rfc_headwater = 'yes'
            else:
                rfc_headwater = 'no'

            # partner scraped metadata
            if get_partner:
                dem_resolution = '{:0.4e}'.format(dem_json['resolution'])
                #dem_date = pd.Timestamp(dem_json['attributes']['AcquisitionDate']).strftime('%Y-%m-%d')
                # some ill-formed dates, for instance for agno3, acquisition date was '0/5/2013', instead just get year
                dem_yr = dem_json['attributes']['AcquisitionDate'][-4:]
                
                fema_df = pd.json_normalize(fema_json, 'features')
                if fema_df.empty:
                    fema_effective_date = ''
                else:
                    # note the epoch time is in milliseconds, so divide by 1000
                    try:
                        fema_effective_date = time.strftime('%Y-%m-%d', time.gmtime(fema_df.loc[fema_df['attributes.EFF_DATE'].idxmax()]['attributes.EFF_DATE']/1000))
                    except:
                        fema_effective_date = ''
                        print(lid + ' malformed epoch?')
                        logging.info(lid + ' has malformed epoch') # msbm8 11/14/2019

            else:
                    dem_resolution = partner_df[partner_df['ahps_lid'] == lid]['dem_resolution'].iloc[0]
                    dem_yr = partner_df[partner_df['ahps_lid'] == lid]['dem_yr'].iloc[0]
                    fema_effective_date = partner_df[partner_df['ahps_lid'] == lid]['fema_eff_date'].iloc[0]
                    
            org_thresh_imp_df = add_meta_cols(thresh_imp_df, threshold_type, max_stg, max_flow, dem_resolution, dem_yr, ahps_fim_exist, usgs_fim_exist, usgs_fim_yr, fema_effective_date, rfc_headwater)
            org_row = pd.DataFrame(add_meta_cols(row, threshold_type, max_stg, max_flow, dem_resolution, dem_yr, ahps_fim_exist, usgs_fim_exist, usgs_fim_yr, fema_effective_date, rfc_headwater)).T

            org_thresh_imp_df.insert(loc=0, column='lid', value=lid)
            lid_df = org_thresh_imp_df.merge(fims_df, left_on='lid', right_on='ahps_lid', how='left').drop('ahps_lid', axis=1)
            
            if external_count == 0 and start_index == 0:
                org_row.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + org_static_fims_fn_suffix), index=False)
                thresh_imp_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + nwps_impact_fn_suffix), index=False)
                lid_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + combined_out_fn_suffix), index=False)
            else:
                org_row.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + org_static_fims_fn_suffix), index=False, mode='a', header=False)
                thresh_imp_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + nwps_impact_fn_suffix), index=False, mode='a', header=False)
                lid_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + combined_out_fn_suffix), index=False, mode='a', header=False)

            external_count += 1

            if (lid_df.empty == False):
                loop_li.append(lid_df)
            site_li.append(org_row)
        elif gage_response.status == 404:
            logging.info(lid + ' has no nwps gauge metadata found, url: ' + gage_url)

        # good for debugging
        if get_partner:
            print(str(i) + ' : ' + aoi + ' - ' + lid + ', dem code: ' + str(dem_response.status))
        else:
            print(str(i) + ' : ' + aoi + ' - ' + lid)

    logging.info('site scraping ended')
    return_df = pd.concat(loop_li)
    org_static_fim_df = pd.concat(site_li)
    return return_df, org_static_fim_df
    
def main():
    with open(os.path.join(ctrl_dir, yaml_fn)) as f:
    # NWRFC settings for request headers, keeping hidden in yaml file
        yaml_data = yaml.full_load(f)
        request_header = {'User-Agent' : yaml_data['user_agent']}

    stage_cols = pd.read_csv(os.path.join(ctrl_dir, stage_columns_fn))
    flow_cols = pd.read_csv(os.path.join(ctrl_dir, flow_columns_fn))

    stage_want_cols = stage_cols.loc[stage_cols['include'] == 'y', 'stage_fim_colnames']
    flow_want_cols = flow_cols.loc[flow_cols['include'] == 'y', 'flow_fim_colnames']

    areas_df = pd.read_csv(os.path.join(ctrl_dir, areas_fn))
    aois_li = areas_df.loc[areas_df['include'] == 'x']['area'].tolist()

    for aoi in aois_li:
        logging.info(aoi + ' metadata gathering has started')
        stage_df = convert_fim_json_df(os.path.join(stage_dir, aoi + stage_fn_suffix))
        flow_df = convert_fim_json_df(os.path.join(flow_dir, aoi + flow_fn_suffix))

        stage_want_df = stage_df.loc[:, stage_want_cols]
        fim_want_df = flow_df.loc[:, flow_want_cols]

        stage_want_org_df = clean_stage_df(stage_want_df)

        # join stage and flow static fim info and remove endlines
        static_fims_df = stage_want_org_df.merge(fim_want_df, on='ahps_lid', suffixes=('_stage', '_flow')).replace(r'\n', '', regex=True).sort_values('ahps_lid')

        all_site_df, org_static_fim_df = get_site_info(static_fims_df, aoi, request_header)

        # writing out site by site instead
        #final_df = site_df.merge(static_fims_df, left_on='lid', right_on='ahps_lid', how='left').drop('ahps_lid', axis=1)
        #final_df.to_csv(out_dir + combined_out_fn, index=False)

        #site_df.to_csv(out_dir + nwps_impact_fn, index=False)
        static_fims_df.to_csv(os.path.join(out_dir, out_fn_prefix + aoi + raw_static_fims_fn_suffix), index=False)

        logging.info(aoi + ' metadata gathering has finished')

    logging.shutdown()

if __name__ == '__main__':
    main()
