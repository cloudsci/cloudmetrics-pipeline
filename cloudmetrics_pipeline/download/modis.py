import datetime
from pathlib import Path
import dateutil
import pytz
import os

from ..scene_extraction import DATETIME_FORMAT
from .sources.worldview import download_rgb_image as worldview_rgb_dl
from .sources.modaps import ModapsClient
from ..process import CloudmetricPipeline


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
    data_path=".",
    satellites=["Terra", "Aqua"],
    products=["Cloud_Mask_1km", "Cloud_Top_Height", "Cloud_Water_Path", "Sensor_Zenith"]
):
    MODAPS_USER = os.environ.get("MODAPS_USER")
    MODAPS_APPKEY = os.environ.get("MODAPS_APPKEY")

    if MODAPS_APPKEY is None or MODAPS_USER is None:
        raise Exception(
            "Please set your MODAPS credentials using the MODAPS_USER and"
            " MODAPS_APPKEY environment variables"
        )

    for satellite in satellites:
        if satellite == "Terra":
            satkey = "MOD"
            instrument = "AM1M"
        elif satellite == "Aqua":
            satkey = "MYD"
            instrument = "PM1M"
        else:
            raise NotImplementedError(satellite)

        kwargs = {
            "instrument": instrument,  # Aqua - PM1M; Terra - AM1M
            "product": f"{satkey}06_L2",  # MODIS L2 Cloud product
            # (MYD - Aqua; MOD - Terra)
            "collection": 61,  # hdf collection (61 for Aqua and Terra)
            "layers": [
                f"{satkey}06_L2___{product}" for product in products
            ],
            "email": MODAPS_USER,
            "appKey": MODAPS_APPKEY,
        }

        modapsClient = ModapsClient()
        order_ids = modapsClient.orderFiles(kwargs)
        print("order_ids=", order_ids)
