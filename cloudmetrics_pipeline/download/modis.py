import datetime
from pathlib import Path
import dateutil
import pytz
import os

from ..scene_extraction import DATETIME_FORMAT
from .sources.worldview import download_rgb_image as worldview_rgb_dl
from modapsclient import ModapsClient
from ..process import CloudmetricPipeline
from .modis_cloudmask import read_MODIS_cloud_mask


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
    """
    MODAPS_EMAIL = os.environ.get("MODAPS_EMAIL")
    MODAPS_TOKEN = os.environ.get("MODAPS_TOKEN")

    if MODAPS_TOKEN is None or MODAPS_EMAIL is None:
        raise Exception(
            "Please set your NASA MODAPS credentials using the MODAPS_EMAIL and"
            " MODAPS_TOKEN environment variables"
        )

    # 2018-03-11 12:00
    MODAPS_DATETIME_FORMAT = "%Y-%m-%d %H:%M"

    modapsClient = ModapsClient()

    for satellite in satellites:
        if satellite == "Terra":
            satkey = "MOD"
        elif satellite == "Aqua":
            satkey = "MYD"
        else:
            raise NotImplementedError(satellite)

        layers = [f"{satkey}06_L2___{product}" for product in products]

        file_ids = modapsClient.searchForFiles(
            products=f"{satkey}06_L2",  # MODIS L2 Cloud product
            startTime=dateutil.parser.parse(start_date).strftime(
                MODAPS_DATETIME_FORMAT
            ),
            endTime=dateutil.parser.parse(end_date).strftime(MODAPS_DATETIME_FORMAT),
            west=bbox[0],
            east=bbox[1],
            south=bbox[2],
            north=bbox[3],
            dayNightBoth="D",
            collection=collection,
        )

        # import ipdb
        # ipdb.set_trace()

        order_ids = ["501640969", "501640970"]

        print(modapsClient.getOrderStatus(order_ids[0]))

        files = modapsClient.fetchFilesForOrder(
            order_id=order_ids[0], auth_token=MODAPS_TOKEN, path=data_path
        )

        cloudmask_fp = None
        for filepath in files:
            if "Cloud_Mask" in filepath.name:
                cloudmask_fp = filepath
                break

        if cloudmask_fp is None:
            raise Exception

        da_cloudmask = read_MODIS_cloud_mask(filepath=cloudmask_fp)
        filepath_nc = cloudmask_fp.parent / cloudmask_fp.name.replace(".hdf", ".nc")
        da_cloudmask.to_netcdf(filepath_nc)

        break

        # order_ids = modapsClient.orderFiles(
        # email=MODAPS_EMAIL,
        # FileIDs=file_ids,
        # doMosaic=True,
        # geoSubsetWest=bbox[0],
        # geoSubsetEast=bbox[1],
        # geoSubsetSouth=bbox[2],
        # geoSubsetNorth=bbox[3],
        # subsetDataLayer=layers,
        # )
        # print("order_ids=", order_ids)

        continue
