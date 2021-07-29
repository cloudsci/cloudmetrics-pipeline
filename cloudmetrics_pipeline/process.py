import yaml
import xarray as xr
from tqdm import tqdm
from pathlib import Path

import cloudmetrics
from .find_scenes import SCENE_PATH, SCENE_DB_FILENAME


def _load_scene_ids(data_path):
    scene_ids_db_filepath = data_path / SCENE_PATH / SCENE_DB_FILENAME
    with open(scene_ids_db_filepath) as fh:
        scenes = yaml.load(stream=fh)
        return scenes


def _compute_metrics_on_cloudmask(da_cloudmask, metrics):
    ds_metrics = xr.Dataset()
    for metric in metrics:
        fn_metric = getattr(cloudmetrics, metric)
        metric_value = fn_metric(cloud_mask=da_cloudmask.values)
        ds_metrics[metric] = metric_value
    return ds_metrics


def main(data_path, metrics=["fractal_dimension"]):
    scene_ids = _load_scene_ids(data_path=data_path)
    for scene_id, source_filepath in tqdm(scene_ids.items()):
        da_cloudmask = xr.open_dataarray(source_filepath)
        ds_metrics = _compute_metrics_on_cloudmask(
            da_cloudmask=da_cloudmask, metrics=metrics
        )
        ds_metrics["scene_id"] = scene_id
        filepath_scene_metrics = data_path / SCENE_PATH / f"{scene_id}.metrics.nc"
        ds_metrics.to_netcdf(filepath_scene_metrics)


if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser()
    argparser.add_argument("--data-path", default=".", type=Path)
    argparser.add_argument("--metrics", default=["fractal_dimension"], nargs="+")
    args = argparser.parse_args()

    main(**vars(args))
