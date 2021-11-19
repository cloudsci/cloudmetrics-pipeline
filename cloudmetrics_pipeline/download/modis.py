import datetime
from pathlib import Path
import dateutil
import pytz

from ..scene_extraction import DATETIME_FORMAT
from .sources.worldview import download_rgb_image as worldview_rgb_dl
from ..process import CloudmetricPipeline
from . import modaps


def _parse_utc_timedate(s):
    d = dateutil.parser.parse(s)
    return d.replace(tzinfo=pytz.utc)


def download_MODIS_RGB_scenes(
    start_date,
    end_date,
    bbox,
    data_path,
    image_format="png",
    satellites=["Terra", "Aqua"],
):
    dt = datetime.timedelta(days=1)

    t = _parse_utc_timedate(start_date)

    file_paths = []
    while t < _parse_utc_timedate(end_date):
        for satellite in satellites:
            filename = f"{t.strftime(DATETIME_FORMAT)}_{satellite}.{image_format}"
            filepath = Path(data_path) / filename

            if not filepath.exists():
                worldview_rgb_dl(
                    filepath=filepath,
                    time=t,
                    bbox=bbox,
                    satellite=f"MODIS_{satellite}",
                    image_format=image_format,
                )

            file_paths.append(filepath)
            t += dt

    return file_paths


def modis_rgb_pipeline(
    start_date,
    end_date,
    bbox,
    data_path=".",
    image_format="png",
    satellites=["Terra", "Aqua"],
):
    """
    Start a pipeline by fetching MODIS true-colour RGB images from NASA WorldView

    start_date:     starting date
    end_date:       end date
    bbox:           bounding-box in WESN format
    data_path:      path where downloaded files will be stored
    image_format:   filetype of downloaded files
    satellites:     list of satellite to download data for (Terra and/or Aqua)
    """

    filepaths = download_MODIS_RGB_scenes(
        start_date=start_date,
        end_date=end_date,
        bbox=bbox,
        data_path=data_path,
        image_format=image_format,
        satellites=satellites,
    )

    return CloudmetricPipeline(source_files=filepaths)


def modis_modaps_pipeline(
    start_date,
    end_date,
    bbox,
    data_path="modaps",
    collection=61,
    satellites=["Terra", "Aqua"],
    products=[
        "Cloud_Mask_1km",
        "Cloud_Top_Height",
        "Cloud_Water_Path",
        "Sensor_Zenith",
    ],
):
    """
    bbox: WESN format
    collection: hdf collection (61 for Aqua and Terra)
    start_date, end_date: time span for query
    products: list of products to download. For example 'Cloud_Mask_1km'
    """
    return modaps.modaps_pipeline(
        start_date=start_date,
        end_date=end_date,
        bbox=bbox,
        collection=collection,
        satellites=satellites,
        products=products,
        data_path=data_path,
    )
