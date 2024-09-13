# original author(s):   henry pai (nwrfc)
# contact info:         henry <dot> pai <at> noaa <dot> gov
# last edit by:         hp
# last edit time:       Sep 2024
# last edit comment:    starting script

# summary: 
# making call field_id ESRI/database call, select * where field_id in ('<id1>', '<id2>', etc.) where ids are nwm segs

import os
import pathlib
import glob
import pandas as pd
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
in_catfim_dir = os.path.join(work_dir, 'out', 'catfim')
in_stats_dir = os.path.join(work_dir, 'out', 'stats')
out_dir = os.path.join(work_dir, "out", "db_calls")

# contorl file indicating wfos/rfcs to scrape
areas_fn = 'nws_aois.csv'

# input file info
catfim_meta_fn_suffix = '_catFim_meta.csv'
stats_fn_suffix = '_usgs_slim_streamstats.csv'

# output files
out_fn_prefix = pd.Timestamp.now().strftime('%Y%m%d') + '_'
out_fn_suffix = '_nwm_aep_stats.txt'

# ===== functions


def main():
    areas_df = pd.read_csv(os.path.join(ctrl_dir, areas_fn))
    aois_li = areas_df.loc[areas_df['include'] == 'x']['area'].tolist()

    for aoi in aois_li:
        catfim_files_li = glob.glob(in_catfim_dir + '/*_' + aoi + catfim_meta_fn_suffix)
        last_catfim_fullfn = max(catfim_files_li, key=os.path.getctime)
        catfim_df = pd.read_csv(last_catfim_fullfn)

        stats_files_li = glob.glob(in_stats_dir + '/*_' + aoi + stats_fn_suffix)
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




