# fim_meta

Scripts are meant to aggregate some metadata from various sources to help more easily evaluate static categorical FIM from NWS (up to HAND FIM v5)

Not all folders/data included in git.  General folder subfolder structure under the working dir (currently labeled as fim_meta from Repo):

- code (included)
- ctrl (included)
- in (not included)
  - flow - manually downloaded from Data Inputs (1.i)
  - stage - manually downloaded from Data Inputs (1.ii)
  - nonhand_fim - manually downloaded from Data Inputs (2 & 3)
  - nwm_aep - manually copy and pasted from ArcGIS Pro AEP layers from Data Input (1.iv), and performed additional attribute table database search
- out (not included)
  - catfim
  - db_calls - for nwm_aep input
  - stats
- logs (not included)

Steps for running:
1. Pull repo
2. Make in, out, logs and associated subdirectories
3. Change control files (in ctrl directory)
   1. Copy/create __config.yaml__ file and specify header to be more descriptive
   2. Change/add Area Of Interest (aoi) in __nws_aois.csv__ with 'x'
   3. Edit columns to retain in __flow_fim_column_ids.csv__ and __stage_fim_column_ids.csv__
4. Run scripts sequentually
   1. __01a_get_static_fim_meta.py__ - inputs (1.i, 1.ii, 2, 3, 4, 5, 7, all control files), outputs (1, 2, 3, 4), issues: USGS DEP query API times out often, overcome options: change __start_index__ variable to last index before timeout, bulk query for Data Input (6), set __get_partner__ to False if have prior downloaded file
   2. __1b_get_usgs_streamstats.py__ - inputs (8, output 2, aoi and yaml control files), outputs (5 & 6)
   3. OPTIONAL if relying on offline ArcGIS Pro SQL search call - this step could go away
      1. __02_make_nwm_aep_call.py__ - inputs (outputs 2 & 6, aoi control file), output (7)
      2. Open ArcGIS Pro -> load FIM layers (specifically Data Input 1.iv) -> select attributes of AEP layers -> search within attribute tables with SQL query for: __feature_id in [Data Output 7]__
      3. Copy and paste attribute table selection into Notepad or Notepad++ (should be tab-delimited) in the __in/nwm_aep__ folder with names __[yyyymmdd]\_[aoi]\_[AEP val]\_nwmAep.txt__
   4. __03_combine_nwm_usgs_stats.py__ - inputs (3, outputs 2 & 6, files from script iii.c, aoi and yaml control files), output (8)

Data inputs:
1. NOAA/NWS ESRI REST services (currently listed public facing service, but limited to FIM30 region)
   1. flow catfim meta:  https://maps.water.noaa.gov/server/rest/services/fim_libs/static_flow_based_catfim/MapServer/0/query
   2. stage catfim meta: https://maps.water.noaa.gov/server/rest/services/fim_libs/static_stage_based_catfim/MapServer/0/query
   3. nwm aep src 1:     https://maps.water.noaa.gov/server/rest/services/reference/static_nwm_flowlines/MapServer/0/query
   4. nwm aep src 2:     login-required rest service
2. json file for ahps fim maps (from Benjamin)
3. json file for usgs fim maps (from Benjamin)
4. NOAA/NWS scraper for the gage thresholds, rating, impact statements, etc.: https://api.water.noaa.gov/nwps/v1/docs/
5. USGS nationalmaps DEM api for point queries: https://epqs.nationalmap.gov/v1/docs
6. USGS nationalmaps DEM api for bulk point query (up to 500 points): https://apps.nationalmap.gov/bulkpqs/#
7. FEMA hazard layer ESRI REST service: https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer
8. USGS streamstats api: https://streamstats.usgs.gov/gagestatsservices/

Data outputs:
1. __out/catfim/[yyyymmdd]\_[aoi]\_raw_catFim_meta.csv__ - all locations from Data Inputs (1.i & 1.ii) with some column merging from flow and stage based categorical FIM
2. __out/catfim/[yyyymmdd]\_[aoi]\_catFim_meta.csv__ - locations that exist on NWPS, pulled from above file (Data Output 1) and adding multiple metadata from NWPS, USGS, FEMA (Data Inputs 2, 3, 4, 5, 7)
3. __out/catfim/[yyyymmdd]\_[aoi]\_impacts_meta.csv__ - combining impacts & thresholds at various stage/flow values along with some point metadata (Data input 4)
4. __out/catfim/[yyyymmdd]\_[aoi]\_catFimReview_meta_stagePrioritized_wExceptions.csv__ - combining bullet Data Output (2 & 3) for final output; stage thresholds are generally prioritized (see Issues below) and exceptions are hardcoded in (so flow threshold site(s))
5. __out/stats/[yyyymmdd]\_[aoi]\_usgs_all_streamstats.csv__ - verbose export for USGS streamstats, generally preferring weighted AEP regression, unless otherwise stated (preference from communication via correspondence with Oregon USGS, but may vary by state).  Note, several Montana sites are downstream of reservoirs and they explicitly AEP is related to regulated and not naturalized flow
6. __out/stats/[yyyymmdd]\_[aoi]\_usgs_slim_streamstats.csv__ - slimmed export for USGS streamstats, removing some metadata
7. __out/db_calls/[yyyymmdd]\_[aoi]\_nwm_aep_stats.csv__ - OPTIONAL, generating query for ArcGIS Pro ESRI call
8. __out/stats/[yyyymmdd]\[aoi]\_[source = online/offline]\_stats_normErr.csv__ - aggregates USGS and NRP AEP's and calculates normalized error to provide some caution noted in Reference 1

References:
1. https://nhess.copernicus.org/articles/19/2405/2019/ - Table 4 highlights error in inundated area with flow error (starts to grow when flow error > 60%)
2. https://www.usgs.gov/3d-elevation-program/about-3dep-products-services - highlights resolutions of 3dep product

Issues:
- DEM query service times out with scraper, currently can rerun from prior scrape
- DEM resolution changes from meter to to arc seconds from Reference 2.  Currently the value mixes meters and arc seconds.  Arc seconds seen in some aoi's so far:
  - 1/3 arc second (9.26e-5, 10 m) is a common type on the west coast
  - 1/9 arc second (3.09e-5, 3 m)
  - 1 arc second (2.78e-4, 30 m)
- no explicit label on threshold type for Data Input (4); tried to handle via algorithm and from primary unit for the observed data, but otherwise stage is prioritized
- checking which NRP AEP source is more correct (1.iii or 1.iv)

TODO:
- [ ] Add ESRI rest call for public flow and stage catfim meta (Data Inputs 1.i and 1.ii) in Step 4.i
- [ ] If Data Input 1.iii is more accurate for NRP AEP, can remove the optional download (Step 3) and hard code Step 4 to neglect offline option
- [ ] Find way to make point DEM 3dep metadata query more robust (not time out)
- [ ] Map DEM resolution to meters
