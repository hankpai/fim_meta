# fim_meta

Not all folders/data included in git.  General folder subfolder structure under the working dir (currently labeled as fim_meta from Repo):

- code (included)
- ctrl (included)
- in (not included)
  - flow - manually downloaded from first bullet below
  - stage - manually downloaded from first bullet below
  - nonhand_fim - manually downloaded from second and third bullet below
- out (not included)
  - catfim
  - stats
- logs (not included)

Data inputs for FIM metadata:
- NOAA/NWS ESRI REST services (currently listed public facing service, but limited to FIM10 region)
  - https://maps.water.noaa.gov/server/rest/services/fim_libs/static_flow_based_catfim/MapServer/0/query
  - https://maps.water.noaa.gov/server/rest/services/fim_libs/static_stage_based_catfim/MapServer/0/query
- json file for ahps fim maps (from Benjamin)
- json file for usgs fim maps (from Benjamin)
- NOAA/NWS scraper for the gage and rating: https://api.water.noaa.gov/nwps/v1/docs/
- USGS nationalmaps DEM api for point queries: https://epqs.nationalmap.gov/v1/docs
- FEMA hazard layer ESRI REST service: https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer

Data outputs for FIM metadata:
- [yyyymmdd]\_[aoi]\_raw_catFim_meta.csv - all locations from first bullet with some column merging from flow and stage based categorical FIM
- [yyyymmdd]\_[aoi]\_catFim_meta.csv - locations that exist on NWPS, pulled from above file and adding multiple metadata from NWPS, USGS, FEMA
- [yyyymmdd]\_[aoi]\_impacts_meta.csv - combining impacts & thresholds at various stage/flow values along with some point metadata
- [yyyymmdd]\_[aoi]\_catFimReview_meta_stagePrioritized_wExceptions.csv - combining bullet 1 and 3 for final output; stage is generally prioritized (see Caveats below) and exceptions are hardcoded in (so flow threshold site(s))

Caveats for FIM metadata:
- no explicit label on threshold type; tried to handle via algorithm and from primary unit for the observed data, but otherwise stage is prioritized

Data inputs from streamstats:
- TBD
