"""
Used resources:

- https://gis.stackexchange.com/questions/328535/opening-eos-netcdf4-hdf5-file-with-correct-format-using-xarray
- https://github.com/pytroll/satpy/blob/e72a3f029e6a94b55f48d894cbad20b1f4ef0562/satpy/etc/readers/modis_l2.yaml
- https://github.com/mapbox/rasterio/issues/2026#issuecomment-720125978
"""
from pathlib import Path
import dateutil
import numpy as np
import os
import yaml
import rioxarray as rxr
import parse
import datetime
import warnings
from tqdm import tqdm

from modapsclient import ModapsClient

from ..utils import dict_to_hash
from ..process import CloudmetricPipeline

# 2018-03-11 12:00
MODAPS_DATETIME_FORMAT = "%Y-%m-%d %H:%M"

QUALTITY_OPTIONS = ["confident_cloudy", "probably_cloudy"]

# MOD06_L2.A2020001.mosaic.061.2021258091607.psmcgscs_000501652987.Cloud_Mask_1km.hdf
FILENAME_FORMAT = "M{platform_id}D06_L2.A{acquisition_date}.mosaic.061.{timestamp}.psmcgscs_{order_id}.{product}.hdf"
# MOD06_L2.A2018001.0950.061.2018003205209.psgscs_000501653312.hdf
FILENAME_FORMAT_SINGLE = (
    "M{platform_id}D06_L2.A{acquisition_time}.061.{timestamp}.psgscs_{order_id}.hdf"
)


def read_bits(value, bit_start, bit_count):
    """
    Read binary fields - see e.g. https://science-emergence.com/Articles/How-to-read-a-MODIS-HDF-file-using-python-/
    Parameters
    ----------
    bit_start : int
        Start bit.
    bit_count : int
        Bit number.
    value : 2D Numpy array
        Binary input field.
    Returns
    -------
    2D Numpy array
        Float field.
    """

    bitmask = pow(2, bit_start + bit_count) - 1
    return np.right_shift(np.bitwise_and(value, bitmask), bit_start)


def read_MODIS_cloud_mask(filepath, quality_flag="confident_cloudy"):
    """
    Read MODIS cloud mask from mosiac'ed M06 HDF file

    returns: xr.DataArray with the cloud mask
    """
    if quality_flag not in QUALTITY_OPTIONS:
        raise NotImplementedError(quality_flag)
    quality_threshold = QUALTITY_OPTIONS.index(quality_flag)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Dataset has no geotransform, gcps, or rpcs. The identity matrix be returned.",
        )
        da = rxr.open_rasterio(filepath)
    assert "MODIS Cloud Mask" in da.attrs.get("long_name")

    # the `x` and `y` coordinates are lon and lat respectively
    da = da.rename(dict(x="lon", y="lat"))

    # the first band contains the quality measure in the first two bits
    da_field_quality_values = read_bits(da.sel(band=1), 1, 2)
    da_cloud_mask = da_field_quality_values <= quality_threshold

    file_meta = parse.parse(FILENAME_FORMAT, filepath.name)
    if file_meta is not None:
        acquisition_time = datetime.datetime.strptime(
            file_meta["acquisition_date"], "%Y%j"
        )
    else:
        file_meta = parse.parse(FILENAME_FORMAT_SINGLE, filepath.name)
        if file_meta is None:
            import ipdb

            ipdb.set_trace()
            raise Exception("Couldn't parse filename format to get meta info")
        acquisition_time = datetime.datetime.strptime(
            file_meta["acquisition_time"], "%Y%j.%H%M"
        )

    da_cloud_mask["time"] = acquisition_time
    da_cloud_mask.name = "cloud_mask"
    da_cloud_mask.attrs["type"] = quality_flag

    return da_cloud_mask


class MODAPSOrderProcessingException(Exception):
    pass


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
        doMosaic=True,
        geoSubsetWest=bbox[0],
        geoSubsetEast=bbox[1],
        geoSubsetSouth=bbox[2],
        geoSubsetNorth=bbox[3],
        subsetDataLayer=layers,
    )

    return order_ids


def modaps_pipeline(
    start_date,
    end_date,
    bbox,
    collection,
    satellites,
    products,
    data_path="modaps.{query_hash}",
):
    """
    Start a cloudmetrics pipeline by fetching data from NASA MODAPS

    bbox: WESN format
    collection: hdf collection (61 for Aqua and Terra)
    start_date, end_date: time span for query
    products: list of products to download. For example 'Cloud_Mask_1km'
    satellites: list of satellites to fetch data for, for example "Terra"
    collection: data collection number to use for example 61 for MODIS
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
        order_data_path = Path(data_path) / order_id
        order_data_path.mkdir(exist_ok=True, parents=True)
        files = modapsClient.fetchFilesForOrder(
            order_id=order_id,
            auth_token=MODAPS_TOKEN,
            path=order_data_path,
        )

        # the files downloaded from MODAPS need further processing
        # for example we want a boolean mask for the MODIS cloudmask
        # TODO: implement postprocessing for other files here too
        for filepath in tqdm(files, desc="post-processing"):
            if filepath.suffix != ".hdf":
                continue

            if (
                "Cloud_Mask" in filepath.name
                or len(products) == 1
                and products[0] == "Cloud_Mask_1km"
            ):
                cloudmask_fp = filepath
                filepath_nc = cloudmask_fp.parent.parent / cloudmask_fp.name.replace(
                    ".hdf", ".nc"
                )
                if not filepath_nc.exists():
                    da_cloudmask = read_MODIS_cloud_mask(filepath=cloudmask_fp)
                    da_cloudmask.to_netcdf(filepath_nc)
                filepaths.append(filepath_nc)
            else:
                # TODO implement conversion to netCDF of other files
                pass
                # filepaths.append(filepath)

    return CloudmetricPipeline(source_files=filepaths)
