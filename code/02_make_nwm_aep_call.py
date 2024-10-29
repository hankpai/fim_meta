# original author(s):   henry pai (nwrfc)
# contact info:         henry <dot> pai <at> noaa <dot> gov
# last edit by:         hp
# last edit time:       Oct 2024
# last edit comment:    small update for file names

# summary: 
# making call field_id ESRI/database call, select * where field_id in ('<id1>', '<id2>', etc.) where ids are nwm segs

import os
import pathlib
import glob
import pandas as pd
import yaml
import pdb

# ===== global/user vars (not path related)

# ===== directories & filenames (site/computer specific)
work_dir = pathlib.Path(__file__).parent.parent  # IDE independent

ctrl_dir = os.path.join(work_dir, "ctrl")   # csv files controlling columns and wfo's to scrape
in_catfim_dir = os.path.join(work_dir, 'out', 'catfim')
in_stats_dir = os.path.join(work_dir, 'out', 'stats')
out_dir = os.path.join(work_dir, "out", "db_calls")

# yaml file
yaml_fn = 'config.yaml'

# contorl file indicating wfos/rfcs to scrape
areas_fn = 'nws_aois.csv'

# input file info
catfim_meta_fn_suffix1 = '_catFim_meta.csv'
stats_fn_suffix1 = '_usgsSlimStats.csv'

# output files
out_fn_prefix = pd.Timestamp.now().strftime('%Y%m%d') + '_'
out_fn_suffix = '_nwm_aep_stats.txt'

with open(os.path.join(ctrl_dir, yaml_fn)) as f:
# NWRFC settings for request headers, keeping hidden in yaml file
# not super happy to make this global
    yaml_data = yaml.full_load(f)
    catfim_meta_fn_suffix2 = '_' + yaml_data['station_src'] + 'Stalist' + catfim_meta_fn_suffix1
    stats_fn_suffix2 = '_' + yaml_data['station_src'] + 'Stalist' + stats_fn_suffix1

# ===== functions
def main():
    areas_df = pd.read_csv(os.path.join(ctrl_dir, areas_fn))
    aois_li = areas_df.loc[areas_df['include'] == 'x']['area'].tolist()

    for aoi in aois_li:
        catfim_files_li = glob.glob(in_catfim_dir + '/*_' + aoi + catfim_meta_fn_suffix2)
        last_catfim_fullfn = max(catfim_files_li, key=os.path.getctime)
        catfim_df = pd.read_csv(last_catfim_fullfn)

        stats_files_li = glob.glob(in_stats_dir + '/*_' + aoi + stats_fn_suffix2)
        last_stats_fullfn = max(stats_files_li, key=os.path.getctime)
        usgs_df = pd.read_csv(last_stats_fullfn)

        nwm_seg_df = usgs_df[['ahps_lid']].merge(catfim_df[['ahps_lid', 'nwm_seg']])

        nwm_segs_li = nwm_seg_df['nwm_seg'].tolist()
        nwm_str1 = ','.join(f"'{str(i)}'" for i in nwm_segs_li)
        final_nwm_str = '(' + nwm_str1 + ')'

        with open(os.path.join(out_dir, out_fn_prefix + aoi + out_fn_suffix), 'w') as f:
            f.write(final_nwm_str)

if __name__ == '__main__':
    main()
