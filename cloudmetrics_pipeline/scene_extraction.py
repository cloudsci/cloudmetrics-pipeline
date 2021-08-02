import yaml
from pathlib import Path
import xarray as xr
import numpy as np
import skimage.io


FILETYPES = dict(image=["png", "jpg", "jpeg"], netcdf=["nc", "nc4"])

DATETIME_FORMAT = "%Y%d%m%H%M"
SCENE_PATH = "cloudmetrics"
SCENE_DB_FILENAME = "scene_ids.yml"


class NoReplaceDict(dict):
    """
    dict-like object which raises exception if a key already exists when doing
    an insert
    """

    class KeyExistsException(Exception):
        pass

    def __setitem__(self, key, val):
        if key in self:
            raise self.KeyExistsException(key)
        dict.__setitem__(self, key, val)


def _make_image_scene(filepath):
    """
    Create a cloud-mask netCDF file from an image using the `greyscale_threshold`
    """
    scene_id = filepath.stem
    image = skimage.io.imread(filepath)
    # ensure the image data is saved so that it renders correctly when plotting
    # the masks later
    image = np.rot90(np.fliplr(image), k=2)
    # cast to int8 here, xarray isn't happy about doing uint8 -> int8 cast
    da = xr.DataArray(image.astype(np.int8))
    filepath_scene = Path(filepath).parent / SCENE_PATH / f"{scene_id}.nc"
    filepath_scene.parent.mkdir(exist_ok=True, parents=True)
    da.to_netcdf(filepath_scene)
    return scene_id, filepath_scene


def _make_netcdf_scenes(filepath):
    scenes = NoReplaceDict()
    # TODO: support for picking a single variable from a dataset
    da = xr.open_dataarray(filepath)

    def _individual_scenes_in_file():
        # TODO: support for usign 1D variables which align with the data but
        # aren't coordinates
        if "scene_id" in da.coords:
            for scene_id in da.scene_id.values:
                yield (scene_id, da.sel(scene_id=scene_id))
        elif "time" in da or "time" in da.coords:
            times_str = da.time.dt.strftime(DATETIME_FORMAT)
            for time in da.time.values:
                time_str = times_str.sel(time=time).item()
                filename_stem = filepath.stem
                scene_id = f"{filename_stem}__{time_str}"
                yield (scene_id, da.sel(time=time))
        else:
            raise NotImplementedError(
                "When using a netCDF file as source it should either"
                "have a `scene_id` or `time` 1D coordinate defined"
            )

    for scene_id, da_scene in _individual_scenes_in_file():
        filename_scene = f"{scene_id}.nc"
        filepath_scene = filepath.parent / SCENE_PATH / filename_scene
        filepath_scene.parent.mkdir(exist_ok=True, parents=True)
        da_scene.to_netcdf(filepath_scene)
        scenes[scene_id] = filepath_scene

    return scenes


def make_scenes(source_files):
    if "*." in source_files:
        path = Path(source_files)
        source_files = list(path.parent.glob(path.name))
    elif isinstance(source_files, str):
        source_files = [source_files]

    source_files = [Path(source_file) for source_file in source_files]

    scenes = NoReplaceDict()

    for filepath in source_files:
        if filepath.suffix[1:] in FILETYPES["image"]:
            scene_id, scene_filepath = _make_image_scene(filepath=filepath)
            scenes[scene_id] = scene_filepath
        elif filepath.suffix[1:] in FILETYPES["netcdf"]:
            scenes.update(_make_netcdf_scenes(filepath=filepath))
        else:
            raise NotImplementedError(filepath.suffix)

    # turn into a regular dict and make paths into strings
    scenes = dict(scenes)
    for scene_id, filepath in scenes.items():
        scenes[scene_id] = str(filepath)

    return scenes


def produce_scene_ids(data_path, dst_filename=SCENE_DB_FILENAME):
    scenes = make_scenes(data_path=data_path)

    with open(data_path / SCENE_PATH / dst_filename, "w") as fh:
        yaml.dump(scenes, fh, default_flow_style=False)


if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser()
    argparser.add_argument("--data-path", default=".", type=Path)
    args = argparser.parse_args()

    produce_scene_ids(**vars(args))
