# original author(s):   henry pai (nwrfc)
# contact info:         henry <dot> pai <at> noaa <dot> gov
# last edit by:         hp
# last edit time:       Sep 2024
# last edit comment:    combining retro nwm aep and compare with usgs given flow errors > 60% lead to noticeable FIM errors

# summary:
# Motivated by Table 4 in https://doi.org/10.5194/nhess-19-2405-2019 - essentially as nMAE rises > 60%, FIM accuracy degrades significantly
# To provide a rough idea of NWM performance, we look at the AEP statistics between the USGS and NWM to see where nMAE may approach > 60%
# and give a better idea of where NWM performance should be used with caution

# things to be careful about:
# - While USGS is considered authoritative, statistical distrutions at higher ends of ratings that themselves contain large measurement errors
# - Additionally, USGS streamstats come in different flavors with assumed most accurate form chosen by 01b script
# - Assumption that NWM AEP stats are correctly estimated (wasn't able to reproduce) from WRDS method described below: 
#   https://vlab.noaa.gov/redmine/projects/wrds/wiki/WRDS_Location_API
# - Finally as both NWM retrospective represents "naturalized" flow as does USGS streamstats, headwater basins likely easier to "naturalize"

# inputs:
# - output file from 01a: catFim_meta
# - output file from 01b: slim_streamstats
# - copied and pasted tab-delimited data (1 file per AEP val) from arcgis pro accessing owp/wpod/gid's FIM AEP libraries

# TODO
# [ ] highlight values of > 60% absolute error

import os
import pathlib
import glob
import pandas as pd
import pdb

# ===== global/user vars (not path related)
# common AEP's of interest, leaving as strings to avoid potential rounding errors in array intersections
aep_li = ['2', '4', '10', '20', '50']

# ===== debugging var

# ===== directories & filenames (site/computer specific)
work_dir = pathlib.Path(__file__).parent.parent  # IDE independent

ctrl_dir = os.path.join(work_dir, "ctrl")   # csv files controlling columns and wfo's to scrape
in_catfim_dir = os.path.join(work_dir, 'out', 'catfim')
in_nwm_aep_dir = os.path.join(work_dir, 'in', 'nwm_aep')
stats_dir = os.path.join(work_dir, "out", "stats")  # both input and output dir

# contorl file indicating wfos/rfcs to scrape
areas_fn = 'nws_aois.csv'

# input file info
catfim_meta_fn_suffix = '_catFim_meta.csv'
usgs_stats_fn_suffix = '_usgs_slim_streamstats.csv'
nwm_aep_fns_suffix = '_nwmAep.txt'

# output files
out_fn_prefix = pd.Timestamp.now().strftime('%Y%m%d') + '_'
nwm_stats_fn_suffix = '_nwmAep.txt'
slim_usgs_fn_suffix = '_usgs_slim_streamstats.csv'

# ===== functions
def org_nwm_aeps(nwm_seg_df, aoi):

    for aep in aep_li:
        
        aep_str = aep.zfill(2)
        nwm_aep_files_li = glob.glob(in_nwm_aep_dir + '/*_' + aoi + '_' + aep_str + nwm_stats_fn_suffix)
        last_nwm_aep_fullfn = max(nwm_aep_files_li, key=os.path.getctime)
        nwm_aep_df = pd.read_csv(last_nwm_aep_fullfn, sep='\t')
        pdb.set_trace()



def main():
    areas_df = pd.read_csv(os.path.join(ctrl_dir, areas_fn))
    aois_li = areas_df.loc[areas_df['include'] == 'x']['area'].tolist()

    for aoi in aois_li:
        # some repetition here with script 02
        catfim_files_li = glob.glob(in_catfim_dir + '/*_' + aoi + catfim_meta_fn_suffix)
        last_catfim_fullfn = max(catfim_files_li, key=os.path.getctime)
        catfim_df = pd.read_csv(last_catfim_fullfn)

        usgs_stats_files_li = glob.glob(stats_dir + '/*_' + aoi + usgs_stats_fn_suffix)
        usgs_last_stats_fullfn = max(usgs_stats_files_li, key=os.path.getctime)
        usgs_df = pd.read_csv(usgs_last_stats_fullfn)

        nwm_seg_df = usgs_df[['ahps_lid']].merge(catfim_df[['ahps_lid', 'nwm_seg']])

        nwm_stats_df = org_nwm_aeps(nwm_seg_df, aoi)


if __name__ == '__main__':
    main()
