# original author(s):   henry pai (nwrfc)
# contact info:         henry <dot> pai <at> noaa <dot> gov
# last edit by:         hp
# last edit time:       Sep 2024

# example usgs streamstats call (taken from nws gage analysis): https://streamstats.usgs.gov/gagestatsservices/statistics?statisticGroups=pfs&stationIDOrCode=14191000
# simplest resource for extracting zarr data (hopefully same for NWM v3.0): https://www.hydroshare.org/resource/c4c9f0950c7a42d298ca25e4f6ba5542/
# other resource for v3: https://www.hydroshare.org/resource/6ca065138d764339baf3514ba2f2d72f/
# nwm retrospective: https://registry.opendata.aws/nwm-archive/
# nwm variables: https://github.com/NOAA-Big-Data-Program/bdp-data-docs/blob/main/nwm/README.md

import os
import yaml
import pathlib
import glob
import pandas as pd
import numpy as np
import json
import logging
import urllib
import urllib3
import fsspec
import pdb

# ===== global/user vars (not path related)
# common AEP's of interest, leaving as strings to avoid potential rounding errors in array intersections
aep_li = ['0.2', '0.5', '1', '2', '4', '10', '20', '50']

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
log_fn = 'streamstats.log'
out_fn_prefix = pd.Timestamp.now().strftime('%Y%m%d') + '_'
out_fn_suffix = '_streamstats.csv'

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
def org_usgs(usgs_json):
    """
    ['id', 'statisticGroupTypeID', 'regressionTypeID', 'stationID', 'value',
       'unitTypeID', 'comments', 'isPreferred', 'citationID',
       'predictionIntervalID', 'statisticErrors', 'statisticGroupType',
       'regressionType', 'predictionInterval', 'yearsofRecord'],
      dtype='object')
    """
    temp_df = pd.DataFrame(usgs_json)
    # taking preferred USGS AEP, note yearsofRecord only taken from empirical AEP (vs. regression/algorithmic AEP)
    # otherwise yearsofRecord should be NA 
    pref_df = temp_df[temp_df['isPreferred']==True][['value', 'yearsofRecord', 'citationID', 'regressionType']]

    stats_meta = pd.DataFrame(list(pref_df['regressionType']))
    # removes AEP, then splits by PK, then replaces underscore with decimal
    aep_percent = stats_meta['code'].str.rstrip('AEP')\
                                    .str.split('PK', expand=True)[1]\
                                    .str.replace('_', '.')

    row_idxs = np.nonzero(np.in1d(aep_percent, aep_li))[0].tolist()  # getting row indices from aep percent to then pluck from perf_df

    org_df = pref_df.iloc[row_idxs][['value', 'yearsofRecord', 'citationID']]
    org_df['aep_percent'] = aep_percent
    org_df['aep_name'] = stats_meta.iloc[row_idxs]['name']

    return_df = org_df
    return(return_df)
    
def get_site_info(mapping_df, request_header):
    """
    loop through getting usgs streamstats and attempted NWM retrospective v3 streamstats
    """
    loop_li = []
    for i, row in mapping_df.iterrows():
        usgs_url = usgs_url_prefix + str(row['usgs_gage'])

        http = urllib3.PoolManager()
        usgs_response = http.request('GET', usgs_url, headers=request_header)
        usgs_json = json.loads(usgs_response.data.decode('utf8'))
        usgs_df = org_usgs(usgs_json)
        pdb.set_trace()

def main():
    with open(os.path.join(ctrl_dir, yaml_fn)) as f:
    # NWRFC settings for request headers, keeping hidden in yaml file
        yaml_data = yaml.full_load(f)
        request_header = {'User-Agent' : yaml_data['user_agent']}
    
    areas_df = pd.read_csv(os.path.join(ctrl_dir, areas_fn))
    aois_li = areas_df.loc[areas_df['include'] == 'x']['area'].tolist()

    for aoi in aois_li:
        logging.info(aoi + ' streamstats gathering has started')
        files_li = glob.glob(in_dir + '/*_' + aoi + catfim_meta_fn_suffix)
        last_catfim_fullfn = max(files_li, key=os.path.getctime)
        catfim_df = pd.read_csv(last_catfim_fullfn)
        usgs_map_df = catfim_df[catfim_df['usgs_gage'] != 0][['ahps_lid', 
                                                              'nwm_seg', 
                                                              'usgs_gage']]

        get_site_info(usgs_map_df, request_header)

if __name__ == '__main__':
    main()
