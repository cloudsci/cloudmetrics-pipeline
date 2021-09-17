import yaml
import xarray as xr
from pathlib import Path
import luigi
import inspect
import numpy as np
import warnings
import hashlib
import shutil

import cloudmetrics
from .scene_extraction import SCENE_PATH, SCENE_DB_FILENAME, make_scenes
from .utils import optional_debugging
from .steps.tile import get_sliding_window_view_strided


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


def _compute_metric_on_cloudmask(da_cloudmask, metric):
    fn_metric = getattr(cloudmetrics, metric)

    def _fn_metric_wrapped(da_cloudmask_):
        return xr.DataArray(fn_metric(da_cloudmask_.squeeze().values), name=metric)

    if "x_dim" in da_cloudmask.attrs and "y_dim" in da_cloudmask.attrs:
        x_dim = da_cloudmask.attrs["x_dim"]
        y_dim = da_cloudmask.attrs["y_dim"]
        da_stacked = da_cloudmask.stack(n=(x_dim, y_dim))
        return da_stacked.groupby("n").apply(_fn_metric_wrapped).unstack("n")
    else:
        return _fn_metric_wrapped(da_cloudmask_=da_cloudmask)


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

        # avoid putting `metric_metric=iorg` in identifier, rather have `metric_iorg`
        params = dict(self.parameters)
        if self.kind in params:
            parts.append(params.pop(self.kind))

        if len(params) > 0:
            param_str = "__".join(f"{k}={v}" for (k, v) in params.items())
            parts.append(param_str)
        if self.fn is not None:
            parts.append(self.fn.__name__)
        return "__".join(parts)

    def run(self):
        with optional_debugging(with_debugger=self.debug):
            self._run()

    def _run(self):
        ds_or_da = self.input().open()
        # ensure the scene id is always set so we can refer to it
        ds_or_da["scene_id"] = self.scene_id

        if self.kind == "mask":
            if isinstance(self.fn, str):
                # TODO: implement shorthand methods like ">0.1"
                raise NotImplementedError(self.fn)
            else:
                if isinstance(ds_or_da, xr.DataArray):
                    da = self.fn(da_scene=ds_or_da, **self.parameters)
                else:
                    da = self.fn(ds_scene=ds_or_da, **self.parameters)
            da.name = "mask"
            da.attrs.update(self.parameters)
            da.attrs["fn"] = self.fn.__name__
        elif self.kind == "metric":
            if isinstance(ds_or_da, xr.DataArray):
                da = ds_or_da
                if da.dtype == np.bool:
                    da_cloudmask = da
                else:
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
                da_cloudmask = ds_or_da.astype(bool)
                da = _compute_metric_on_cloudmask(
                    da_cloudmask=da_cloudmask, metric=self.parameters["metric"]
                )
            else:
                raise Exception(
                    "Before computing metrics you need to add a `.mask` operation"
                    " to the pipeline. Currently you are trying to compute metrics"
                    " on a dataset with the following variables: {', '.join(ds_or_da.data_vars.keys())}"
                )
        elif self.kind == "tile":
            da = get_sliding_window_view_strided(ds_or_da, **self.parameters)
        else:
            raise NotImplementedError(self.kind)

        Path(self.output().fn).parent.mkdir(exist_ok=True, parents=True)
        da.to_netcdf(self.output().fn)

    @property
    def scene_id(self):
        filepath_parent = Path(self.parent.output().fn)
        return filepath_parent.stem

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

    def tile(self, window_size, window_stride=None, window_offset="stride_center"):
        """ """
        kwargs = dict(
            window_size=window_size,
            window_stride=window_stride,
            window_offset=window_offset,
        )
        step = dict(kind="tile", parameters=kwargs)
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
                f"The {', '.join(missing_metrics)} metric isn't implemented, the"
                f" available metrics are: {', '.join(AVAILABLE_METRICS)}"
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

    def execute(self, parallel_tasks=1, debug=False, clean=False):
        """
        Execute the pipeline with `parallel_tasks` number of tasks (if >1 then
        and instance of `luigid` must be running) and optionally with debugging
        (only possible when executing a single task at a time)

        clean: remove all intermediate pipeline files. You should use this
               while you are still modifying any functions you're passing into
               the pipeline to avoid output from previous versions being cached
        """
        if clean and Path(SCENE_PATH).exists():
            # TODO: make this only remove the actual files created by the
            # pipeline and not just everything in `cloudmetrics/`
            shutil.rmtree(SCENE_PATH)

        # in the first step we need to split the source files into individual
        # scenes
        with optional_debugging(with_debugger=debug):
            scenes = make_scenes(source_files=self._source_files)

            if len(scenes) == 0:
                raise Exception("No scenes found")

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
                # there is a parent task for each scene
                for parent_task in parent_tasks:
                    if step["kind"] == "metrics":
                        # shorthand which compute more than one metric, but we
                        # want to queue more than one task for this
                        for metric in step["parameters"]["metrics"]:
                            parameters = dict(metric=metric)
                            task = PipelineStep(
                                parent=parent_task,
                                debug=debug,
                                kind="metric",
                                parameters=parameters,
                            )
                            tasks.append(task)
                    else:
                        task = PipelineStep(parent=parent_task, debug=debug, **step)
                        tasks.append(task)

                # the new set of tasks become the new parent tasks
                parent_tasks = tasks
                tasks = []

        outputs = self._run_tasks(tasks=parent_tasks, parallel_tasks=parallel_tasks)

        return self._merge_outputs(outputs=outputs)

    def _make_pipeline_id(self, tasks):
        task_identifiers = []
        for task in tasks:
            t_id = task.output().fn
            task_identifiers.append(t_id)

        s = "__".join(sorted(task_identifiers))
        s_hash = hashlib.md5(s.encode("utf-8")).hexdigest()

        return s_hash

    def _store_output(self, data_path, ds_merged, identifier, parent_tasks):
        identifier = self._make_pipeline_id(tasks=parent_tasks)

        # TODO: put this into a luigi pipeline instead
        fn_out = f"data-{identifier}.nc"
        p_out = data_path / SCENE_PATH / fn_out
        ds_merged.to_netcdf(p_out)

    def _merge_outputs(self, outputs):
        das = [output.open() for output in outputs]
        if len(set([da.name for da in das])):
            das_by_name = {}
            for da in das:
                das_by_name.setdefault(da.name, []).append(da)

            das_merged = []
            for name, das_with_name in das_by_name.items():
                das_merged.append(xr.concat(das_with_name, dim="scene_id"))

            ds_merged = xr.merge(das_merged)
        else:
            ds_merged = xr.concat(das, dim="scene_id")

        if len(ds_merged.data_vars) == 1:
            name = list(ds_merged.data_vars)[0]
            da = ds_merged[name]
            da.name = name
            return da
        else:
            return ds_merged


def find_scenes(source_files):
    """
    Start a pipeline from the provided source files. These can either be given
    as a glob pattern (e.g. "rico_*.nc"), a list of filenames or a single
    filename.
    """
    return CloudmetricPipeline(source_files=source_files)
