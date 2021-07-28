# cloudmetrics

Data processing pipeline for the
[cloudmetrics](https://github.com/cloudsci/cloudmetrics) package.

**NOTE**: the documentation below is currently evolving

Using the pipeline is comprised of three steps

1. (Optionally downloading) and checking the source data, and from this
   generating the scene IDs (see below)

2. Computing the metrics


## Source data formats

The cloud metrics pipeline operates on data directories containing 2D
datasets, either stored as images (`.jpeg`, `.png`) or as netCDF files
(`.nc`). A dataset is assumed to be comprised of multiple "scenes" each
with a unique `scene_id`. For image-files the scene id will be defined as
the filename without the file extension (i.e. `{scene_id}.png`) and for
netCDF files the scene id will either use the `scene_id` variable (if it
exist) or will be generated from the `time` coordinate (with the format
"YYYYMMDDhhmm"). For netCDF files either the `scene_id` variable or `time`
coordinate must exist.
