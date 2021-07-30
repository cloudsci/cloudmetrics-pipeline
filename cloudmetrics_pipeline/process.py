import yaml
import xarray as xr
from tqdm import tqdm
from pathlib import Path
import luigi
import inspect
import numpy as np
import warnings

import cloudmetrics
from .scene_extraction import SCENE_PATH, SCENE_DB_FILENAME, make_scenes
from .utils import optional_debugging


AVAILABLE_METRICS = [
    name for name, _ in inspect.getmembers(cloudmetrics, inspect.isfunction)
]


class XArrayTarget(luigi.target.FileSystemTarget):
    fs = luigi.local_target.LocalFileSystem()

    def __init__(self, path, *args, **kwargs):
        super(XArrayTarget, self).__init__(path, *args, **kwargs)
        self.path = path

    def open(self, *args, **kwargs):
        ds = xr.open_dataset(self.path, *args, **kwargs)

        if len(ds.data_vars) == 1:
            name = list(ds.data_vars)[0]
            da = ds[name]
            da.name = name
            return da
        else:
            return ds

    @property
    def fn(self):
        return self.path


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


class SourceFile(luigi.Task):
    filepath = luigi.Parameter()

    def output(self):
        fpath = Path(self.filepath)
        if not fpath.suffix == ".nc":
            raise NotImplementedError(fpath.suffix)
        return XArrayTarget(self.filepath)


class PipelineStep(luigi.Task):
    kind = luigi.Parameter()
    parent = luigi.Parameter()
    fn = luigi.Parameter(default=None)
    parameters = luigi.DictParameter(default={})
    debug = luigi.BoolParameter(default=False)

    def requires(self):
        if isinstance(self.parent, luigi.Task):
            return self.parent
        else:
            return SourceFile(filepath=self.parent)

    @property
    def identifier(self):
        parts = [self.kind]
        if len(self.parameters) > 0:
            param_str = "_".join(f"{k}={v}" for (k, v) in self.parameters.items())
            parts.append(param_str)
        if self.fn is not None:
            parts.append(self.fn.__name__)
        return "_".join(parts)

    def run(self):
        with optional_debugging(with_debugger=self.debug):
            self._run()

    def _run(self):
        ds_or_da = self.input().open()

        if self.kind == "mask":
            if isinstance(self.fn, str):
                # TODO: implement shorthand methods like ">0.1"
                raise NotImplementedError(self.fn)
            else:
                if isinstance(ds_or_da, xr.DataArray):
                    da = self.fn(da_scene=ds_or_da, **self.parameters)
                else:
                    da = self.fn(ds_scene=ds_or_da, **self.parameters)
        elif self.kind == "metrics":
            if isinstance(ds_or_da, xr.DataArray):
                da = ds_or_da
                if da.dtype == np.bool:
                    da_cloudmask = da
                elif da.dtype in [np.float32, np.float64]:
                    v_min = da.min()
                    v_max = da.max()
                    if v_min != 0.0 or v_max != 1.0:
                        raise Exception(
                            "The field you're trying use as a mask appears"
                            " to not only contain 0.0 and 1.0 values"
                            f" {da.name}=[{v_min.item()}:{v_max.item()}]"
                            " Maybe you forgot to apply a mask method?"
                        )
                    else:
                        da_cloudmask = da.astype(bool)
                else:
                    raise NotImplementedError(da.dtype)
                da_cloudmask = ds_or_da.astype(bool)
                da = _compute_metrics_on_cloudmask(
                    da_cloudmask=da_cloudmask, metrics=self.parameters["metrics"]
                )
            else:
                raise Exception(
                    "Before computing metrics you need to add a `.mask` operation"
                    " to the pipeline. Currently you are trying to compute metrics"
                    " on a dataset with the following variables: {', '.join(ds_or_da.data_vars.keys())}"
                )
        else:
            raise NotImplementedError(self.kind)

        Path(self.output().fn).parent.mkdir(exist_ok=True, parents=True)
        da.to_netcdf(self.output().fn)

    def output(self):
        filepath_parent = Path(self.parent.output().fn)
        filepath_out = filepath_parent.parent / self.identifier / filepath_parent.name

        return XArrayTarget(str(filepath_out))


class CloudmetricPipeline:
    def __init__(self, source_files, steps=[]):
        self._source_files = source_files
        self._steps = steps

    def _add_step(self, step):
        steps = self._steps + [
            step,
        ]
        return CloudmetricPipeline(source_files=self._source_files, steps=steps)

    def mask(self, fn, **kwargs):
        """
        Compute a cloud-mask using function `fn` passing in extra keyword
        arguments **kwargs
        """
        step = dict(kind="mask", fn=fn, parameters=kwargs)
        return self._add_step(step=step)

    def compute_metrics(self, metrics):
        """
        Compute all `metrics`
        """
        missing_metrics = [
            metric for metric in metrics if metric not in AVAILABLE_METRICS
        ]
        if len(missing_metrics):
            raise NotImplementedError(
                f"The {', '.join(missing_metrics)} aren't implement, the available"
                f" metrics are: {', '.join(AVAILABLE_METRICS)}"
            )
        step = dict(kind="metrics", parameters=dict(metrics=metrics))
        return self._add_step(step=step)

    def _run_tasks(self, tasks, parallel_tasks):
        if parallel_tasks == 1:
            success = luigi.build(tasks, local_scheduler=True)
        else:
            success = luigi.build(tasks, workers=parallel_tasks)

        if success:
            return [t.output() for t in tasks]
        else:
            raise Exception("Error occoured while executing pipeline")

    def execute(self, parallel_tasks=1, debug=False):
        """
        Execute the pipeline with `parallel_tasks` number of tasks (if >1 then
        and instance of `luigid` must be running) and optionally with debugging
        (only possible when executing a single task at a time)
        """
        # in the first step we need to split the source files into individual
        # scenes
        with optional_debugging(with_debugger=debug):
            scenes = make_scenes(source_files=self._source_files)

        if debug and parallel_tasks != 1:
            raise Exception("Debugging is only possible when executing in serial mode")

        parent_tasks = [
            SourceFile(filepath=str(filename)) for filename in scenes.values()
        ]

        with warnings.catch_warnings():
            # luigi doens't like us putting functions into the parameters, but
            # it works so let's ignore the warnings for now :)
            warnings.simplefilter("ignore")
            tasks = []
            for step in self._steps:
                tasks = []
                for parent_task in parent_tasks:
                    task = PipelineStep(parent=parent_task, debug=debug, **step)
                    tasks.append(task)
                parent_tasks = tasks

        _ = self._run_tasks(tasks=tasks, parallel_tasks=parallel_tasks)
        # TODO: write routine to aggregate output into named file


def find_scenes(source_files):
    """
    Start a pipeline from the provided source files. These can either be given
    as a glob pattern (e.g. "rico_*.nc"), a list of filenames or a single
    filename.
    """
    return CloudmetricPipeline(source_files=source_files)


if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser()
    argparser.add_argument("--data-path", default=".", type=Path)
    argparser.add_argument("--metrics", default=["fractal_dimension"], nargs="+")
    args = argparser.parse_args()

    main(**vars(args))
