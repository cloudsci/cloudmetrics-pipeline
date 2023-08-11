# cloudmetrics pipeline

Data processing pipeline for the
[cloudmetrics](https://github.com/cloudsci/cloudmetrics) package.

**NOTE**: the documentation below is currently evolving

# Installation

```shell
pip install git+https://github.com/cloudsci/cloudmetrics-pipeline.git
```

# Usage

To use the pipeline you simply start with
`.find_scenes(source_files=...)`, chain the steps of masking, cropping,
computing metrics and finally you execute the pipeline.

```python
import cloudmetrics_pipeline as cm_pipeline

def cloud_mask(da_scene):
    assert da_scene.name == "lwp"
    return da_scene > 0.0

da_iorg = (
    cm_pipeline
    .find_scenes(source_files="*.nc")
    .mask(fn=cloud_mask)
    .compute_metrics(metrics=["iorg"])
    .execute()
)
```

## Source data formats

The cloud metrics pipeline operates on data directories containing 2D datasets,
either stored as images (.jpeg, .png) or as netCDF files (.nc). A dataset is
assumed to be comprised of multiple "scenes" each with a unique scene_id.
The provided source files will be split into individual netCDF files each
containing a single scene (for images the pixel values will be turned into 3D
`xarray.DataArray`s).

For image-files the scene id will be defined as the filename without the file
extension (i.e. `{scene_id}.png`) and for netCDF files the scene id will either
use the `scene_id` variable (if it exist) or will be generated from the time
coordinate (with the format `YYYYMMDDhhmm`). For netCDF files either the
`scene_id` variable or time coordinate must exist.

To start a pipeline from a set of existing source files you will use
`cloudmetrics_pipeline.find_scenes(source_files=...)`. If your files don't
already contain `scene_id` or `time` coordinates you can provide a
preprocessing function through the argument `.find_source_files(preprocess=fn)`
which will be called with each loaded file. You can then use this function to
add the necessary `scene_id`.

## Parallel execution

The pipeline execution is built on [luigi](https://luigi.readthedocs.io)
and may be executed in parallel by starting a `luigid` server (simply run
`luigid` in a separate terminal session) and providing the number of
parallel workers to use when running `.execute(..)`, e.g.
`.execute(parallel_tasks=4)` for 4 parallel tasks.

## Masking

If the source files used aren't already boolean masks (in which case no
masking will be needed) you will need to add
a `.mask(fn={masking_function})` step to your pipeline before computing
any metrics. To the call you can provide any function you like that
returns a boolean mask. Depending on whether the source dataset contains
a single or multiple variables the mask function you provide should take
either `da_scene` or `ds_scene` as an argument. Any additional arguments
you provide to `.mask(...)` will be also be passed to the masking funciton
you provided (this can be useful for doing parameter studies, see below).


```python
import numpy as np
import cloudmetrics_pipeline as cm_pipeline


def cloudy_updraft_mask(ds_scene, w_threshold):
    updraft_mask = ds_scene.w > w_threshold
    cloudy_mask = ds_scene.lwp > 0.0
    cloud_mask = np.logical_and(updraft_mask, cloudy_mask)
    return cloud_mask

(
    cm_pipeline
    .find_scenes(source_files="simulation_output.nc")
    .mask(fn=cloudy_updraft_mask, w_threshold=0.1)
    .compute_metrics(metrics=["iorg"])
    .execute()
)
```


## Parameter studies

Because the pipeline is a python object you can assign in to a variable
and branch at any point to do multiple different passes with different
values for any parameter. For example one might want to try multiple
different threshold values on liquid-water path:


```python
import cloudmetrics_pipeline as cm_pipeline


def cloudy_updraft_mask(da_scene, lwp_threshold):
    assert da_scene.name == "lwp"
    return da_scene > lwp_threshold


# create the start of the pipeline which will be the same every time
pipeline_scenes = cm_pipeline.find_scenes(source_files=[...])

for lwp_threshold in [0.0, 0.1, 0.2]:
    (
        pipeline_scenes
        .mask(fn=cloudy_updraft_mask, lwp_threshold=lwp_threshold)
        .compute_metrics(metrics=["iorg"])
        .execute()
    )
```


## Cropping

*Not yet implemented*

There will be eventually be a `.crop(...)` function added to the pipeline
