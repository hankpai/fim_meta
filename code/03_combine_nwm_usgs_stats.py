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
#   (i believe some streamstats pass uncertainty bands defined, at least for the empirically derived method)
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
import functools
import operator
import pdb

# ===== global/user vars (not path related)
# common AEP's of interest, leaving as strings to avoid potential rounding errors in array intersections
aep_li = ['2', '4', '10', '20', '50']
usgs_keep_cols = ['ahps_lid', 'wfo', 'rfc_headwater', 'nwm_streamOrder', 'usgs_stat_type', 'ratingMax_cfs']

# ===== debugging var

# ===== directories & filenames (site/computer specific)
work_dir = pathlib.Path(__file__).parent.parent  # IDE independent

ctrl_dir = os.path.join(work_dir, "ctrl")  # csv files controlling columns and wfo's to scrape
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
out_fn_suffix = '_stats_normErr.csv'

# ===== functions
def org_nwm_aeps(nwm_seg_df, aoi):
    """
    inputs: aep (one percentile value at a time) copy & paste nwm files, region of interest
    output: df of just nwm segment and flow associated with aep val 
    challenge: some duplicates due to more hydro_id's than nwm_segments, assume's same streamflow @ aep per hydro_id
    """
    
    # columns from nwm aep copied & pasted as a tab delimited file from arcgis pro from gid's rest service(s)
    # - NWM Feature ID
    # - Hydro ID
    # - USGS HUC8
    # - Streamflow (cfs)
    # - FIM Stage (ft)
    # - FIM Version
    # - Branch
    # - Max Rating Curve Stage (ft)
    # - Max Rating Curve Streamflow (cfs)
    # - oid
    # - geom

    loop_li = []

    for i, aep in enumerate(aep_li):
        # grabbing most recent copy and paste files per aep
        aep_str = aep.zfill(2)
        nwm_aep_files_li = glob.glob(in_nwm_aep_dir + '/*_' + aoi + '_' + aep_str + nwm_aep_fns_suffix)
        last_nwm_aep_fullfn = max(nwm_aep_files_li, key=os.path.getctime)
        nwm_aep_df = pd.read_csv(last_nwm_aep_fullfn, sep='\t')

        # multiple hydro_ids, so getting unique segments.
        # ASSUMPTION: only one aep streamflow per nwm segment
        unique_nwm_aep_df = nwm_aep_df[['NWM Feature ID','Streamflow (cfs)']].drop_duplicates(subset='NWM Feature ID')
        unique_nwm_aep_df.columns = ['nwm_seg', aep_str + '_nwm']

        lid_nwm_aep_df = nwm_seg_df.merge(unique_nwm_aep_df, how='left').drop('nwm_seg', axis=1).set_index('ahps_lid')

        loop_li.append(lid_nwm_aep_df)

    # merging/concatenating
    return_df = pd.concat(loop_li, axis=1)

    return(return_df)

def calc_norm_err(usgs_df, nwm_df):
    """
    calc norm_error, straightforward but also renaming columns
    """
    usgs_rename_df = usgs_df.set_axis(usgs_df.columns.str.removesuffix('_usgs'), axis=1)
    nwm_rename_df = nwm_df.set_axis(nwm_df.columns.str.removesuffix('_nwm'), axis=1)

    norm_err_df = round(((nwm_rename_df - usgs_rename_df)/usgs_rename_df * 100), 1) 
    return_df = norm_err_df.add_suffix('_normErr')

    return(return_df)

def main():
    areas_df = pd.read_csv(os.path.join(ctrl_dir, areas_fn))
    aois_li = areas_df.loc[areas_df['include'] == 'x']['area'].tolist()

    usgs_aep_cols_li = [format(float(i), '.1f') for i in aep_li]
    usgs_aep_rename_li = [i.zfill(2) + '_usgs' for i in aep_li]

    for aoi in aois_li:
        # some repetition here with script 02
        catfim_files_li = glob.glob(in_catfim_dir + '/*_' + aoi + catfim_meta_fn_suffix)
        last_catfim_fullfn = max(catfim_files_li, key=os.path.getctime)
        catfim_df = pd.read_csv(last_catfim_fullfn)

        usgs_stats_files_li = glob.glob(stats_dir + '/*_' + aoi + usgs_stats_fn_suffix)
        usgs_last_stats_fullfn = max(usgs_stats_files_li, key=os.path.getctime)
        usgs_df = pd.read_csv(usgs_last_stats_fullfn)

        # selecting aep's of interest (leaves out 0.2 and 1), reduce and add help flatten lists
        usgs_aep_df = usgs_df[functools.reduce(operator.add, [usgs_keep_cols, usgs_aep_cols_li])] 

        # renaming usgs cols, 2nd answer: https://stackoverflow.com/questions/47343838/how-to-change-column-names-in-pandas-dataframe-using-a-list-of-names 
        usgs_org_df = usgs_aep_df.rename(columns=dict(zip(usgs_aep_cols_li, usgs_aep_rename_li))).set_index('ahps_lid')

        nwm_seg_df = usgs_df[['ahps_lid']].merge(catfim_df[['ahps_lid', 'nwm_seg']])

        nwm_stats_df = org_nwm_aeps(nwm_seg_df, aoi)

        merged_df = usgs_org_df.merge(nwm_stats_df, left_index=True, right_index=True)
        
        usgs_slim_df = usgs_org_df[usgs_aep_rename_li]
        norm_error_df = calc_norm_err(usgs_slim_df, nwm_stats_df)

        final_df = merged_df.merge(norm_error_df, left_index=True, right_index=True)
        final_df.to_csv(os.path.join(stats_dir, out_fn_prefix + aoi + out_fn_suffix))

if __name__ == '__main__':
    main()
