import datetime
from pathlib import Path
import dateutil
import pytz
import os
import yaml

from ..scene_extraction import DATETIME_FORMAT
from .sources.worldview import download_rgb_image as worldview_rgb_dl
from modapsclient import ModapsClient
from ..process import CloudmetricPipeline
from .modis_cloudmask import read_MODIS_cloud_mask
from ..utils import dict_to_hash

# 2018-03-11 12:00
MODAPS_DATETIME_FORMAT = "%Y-%m-%d %H:%M"


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


def _modaps_query_and_order(
    modapsClient,
    satellite,
    products,
    start_date,
    end_date,
    bbox,
    collection,
    MODAPS_EMAIL,
):
    """
    Query modaps and place an order for all the matching files
    """
    if satellite == "Terra":
        satkey = "MOD"
    elif satellite == "Aqua":
        satkey = "MYD"
    else:
        raise NotImplementedError(satellite)

    layers = [f"{satkey}06_L2___{product}" for product in products]

    file_ids = modapsClient.searchForFiles(
        products=f"{satkey}06_L2",  # MODIS L2 Cloud product
        startTime=dateutil.parser.parse(start_date).strftime(MODAPS_DATETIME_FORMAT),
        endTime=dateutil.parser.parse(end_date).strftime(MODAPS_DATETIME_FORMAT),
        west=bbox[0],
        east=bbox[1],
        south=bbox[2],
        north=bbox[3],
        dayNightBoth="D",
        collection=collection,
    )

    order_ids = modapsClient.orderFiles(
        email=MODAPS_EMAIL,
        FileIDs=file_ids,
        doMosaic=False,
        geoSubsetWest=bbox[0],
        geoSubsetEast=bbox[1],
        geoSubsetSouth=bbox[2],
        geoSubsetNorth=bbox[3],
        subsetDataLayer=layers,
    )

    return order_ids


class MODAPSOrderProcessingException(Exception):
    pass


def modis_modaps_pipeline(
    start_date,
    end_date,
    bbox,
    data_path="modaps.{query_hash}",
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
    query_hash = dict_to_hash(locals())
    data_path = Path(data_path.format(**locals()))

    MODAPS_EMAIL = os.environ.get("MODAPS_EMAIL")
    MODAPS_TOKEN = os.environ.get("MODAPS_TOKEN")

    if MODAPS_TOKEN is None or MODAPS_EMAIL is None:
        raise Exception(
            "Please set your NASA MODAPS credentials using the MODAPS_EMAIL and"
            " MODAPS_TOKEN environment variables. You can create a new account for"
            " NASA LAADS DAAC on https://ladsweb.modaps.eosdis.nasa.gov/"
            " and generate a key on https://ladsweb.modaps.eosdis.nasa.gov/profile/#app-keys"
        )

    modapsClient = ModapsClient()

    order_reference_filepath = data_path / "modaps_orders.yml"

    if not order_reference_filepath.exists():
        order_ids = []
        for satellite in satellites:
            order_ids += _modaps_query_and_order(
                modapsClient=modapsClient,
                satellite=satellite,
                products=products,
                start_date=start_date,
                end_date=end_date,
                bbox=bbox,
                collection=collection,
                MODAPS_EMAIL=MODAPS_EMAIL,
            )

        if len(order_ids) > 0:
            order_reference_filepath.parent.mkdir(exist_ok=True, parents=True)
            with open(order_reference_filepath, "w") as fh:
                yaml.dump(order_ids, fh)
            raise MODAPSOrderProcessingException(
                "MODAPS data order was placed, please wait a few minutes"
                " for the order to be processed before running the pipeline again"
            )
        else:
            raise Exception("No files were found matching the query parameters")

    # if we reach this stage we know that a file containing the order ids exists
    with open(order_reference_filepath) as fh:
        order_ids = yaml.load(fh)

    order_statuses = {
        order_id: modapsClient.getOrderStatus(order_id) for order_id in order_ids
    }
    unique_statuses = tuple(set(order_statuses.values()))

    if len(unique_statuses) > 1 or unique_statuses[0] != "Available":
        print(order_statuses)
        raise MODAPSOrderProcessingException(
            "Some MODAPS orders are still processing. Please wait a while and"
            " then try running the pipeline again. You can check details on"
            " your orders on https://ladsweb.modaps.eosdis.nasa.gov/search/history"
        )

    filepaths = []
    for order_id in order_ids:
        files = modapsClient.fetchFilesForOrder(
            order_id=order_id,
            auth_token=MODAPS_TOKEN,
            path=Path(data_path) / order_id,
        )

        # the files downloaded from MODAPS need further processing
        # for example we want a boolean mask for the MODIS cloudmask
        # TODO: implement postprocessing for other files here too
        for filepath in files:
            if "Cloud_Mask" in filepath.name:
                cloudmask_fp = filepath

                da_cloudmask = read_MODIS_cloud_mask(filepath=cloudmask_fp)
                filepath_nc = cloudmask_fp.parent.parent / cloudmask_fp.name.replace(
                    ".hdf", ".nc"
                )
                da_cloudmask.to_netcdf(filepath_nc)
                filepaths.append(filepath_nc)
            else:
                # TODO implement conversion to netCDF of other files
                pass
                # filepaths.append(filepath)

    return CloudmetricPipeline(source_files=filepaths)
