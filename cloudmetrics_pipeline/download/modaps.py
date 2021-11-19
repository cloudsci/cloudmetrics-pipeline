"""
Used resources:

- https://gis.stackexchange.com/questions/328535/opening-eos-netcdf4-hdf5-file-with-correct-format-using-xarray
- https://github.com/pytroll/satpy/blob/e72a3f029e6a94b55f48d894cbad20b1f4ef0562/satpy/etc/readers/modis_l2.yaml
- https://github.com/mapbox/rasterio/issues/2026#issuecomment-720125978
<<<<<<< HEAD
=======
- https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/lws-classic/api.html
>>>>>>> a9a3a50585b34b67f3a7d27084e5aa4bc358caf4
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
import itertools
from tqdm import tqdm

from modapsclient import ModapsClient

from ..utils import dict_to_hash
from ..process import CloudmetricPipeline

# 2018-03-11 12:00
MODAPS_DATETIME_FORMAT = "%Y-%m-%d %H:%M"
MODAPS_DATE_FORMAT = "%Y-%m-%d"

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


MODAPS_SATELITES = dict(
    Terra="MOD",
    Aqua="MYD",
)


def _modaps_query(
    satellite,
    products,
    start_date,
    end_date,
    bbox,
    collection,
):
    """
    Query modaps and place an order for all the matching files
    """
    modapsClient = ModapsClient()

    satkey = MODAPS_SATELITES.get(satellite)
    if satkey is None:
        raise NotImplementedError(satellite)

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

    return file_ids


def _modaps_order(
    satellite,
    products,
    bbox,
    file_ids,
    MODAPS_EMAIL,
):
    """
    Place an order for all the matching files and return the order id
    """
    modapsClient = ModapsClient()

    satkey = MODAPS_SATELITES.get(satellite)
    if satkey is None:
        raise NotImplementedError(satellite)

    layers = [f"{satkey}06_L2___{product}" for product in products]

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
    if len(order_ids) != 1:
        raise NotImplementedError(order_ids)

    return order_ids[0]


class YamlDB:
    """Key-value store using YAML-file for storage"""

    def __init__(self, name, path):
        self.path = Path(path) / f"{name}.yml"
        self._update_from_disc()

    def _update_from_disc(self):
        if self.path.exists():
            with open(self.path) as fh:
                self._data = yaml.load(fh)
        else:
            self._data = {}

    def _write_to_disc(self):
        self.path.parent.mkdir(exist_ok=True, parents=True)
        with open(self.path, "w") as fh:
            yaml.dump(self._data, fh)

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value
        self._write_to_disc()

    def remove(self, key):
        del self._data[key]
        self._write_to_disc()

    @property
    def keys(self):
        return list(self._data.keys())


def _create_query_date_intervals(start_date, end_date):
    """
    To reduce the amount of data in each request we group make a query for each
    week worths of data to be downloaded
    """
    d = start_date

    while d < end_date:
        # calculate start of next week
        d_isocal = d.isocalendar()
        weekday_offset = d_isocal.weekday - 1
        d_next = d + datetime.timedelta(days=7 - weekday_offset)

        d_end = d_next - datetime.timedelta(days=1)

        if d_end > end_date:
            d_end = end_date

        yield d, d_end

        d = d_next


def _modaps_query_and_order(
    satellite,
    start_date,
    end_date,
    bbox,
    collection,
    products,
    MODAPS_EMAIL,
    query_db,
    order_db,
    file_db,
    data_path,
):
    """
    query for data and then order any that hasn't already been ordered
    """
    modapsClient = ModapsClient()

    query_kwargs = dict(
        satellite=satellite,
        products=products,
        start_date=start_date,
        end_date=end_date,
        bbox=bbox,
        collection=collection,
    )
    query_hash = dict_to_hash(query_kwargs)
    file_ids = query_db.get(query_hash)

    if file_ids is None:
        # looks like this query hasn't been done before, let's do it now so
        # we know what files to download
        file_ids = _modaps_query(**query_kwargs)
        query_db.set(query_hash, file_ids)

        if len(file_ids) == 0:
            raise Exception("No files were found matching the query parameters")

    # ensure we have file_id -> filename for all the file_ids in the returned
    # query, we will use this to check for files which have been downloaded
    file_ids_missing_properties = [
        file_id for file_id in file_ids if file_db.get(file_id) is None
    ]

    if len(file_ids_missing_properties) > 0:
        for file_properties in modapsClient.getFileProperties(
            file_ids_missing_properties
        ):
            file_db.set(file_properties["fileId"], file_properties["fileName"])

    # filter out any files that we have already downloaded
    def file_has_been_downloaded(file_id):
        filename = Path(file_db.get(file_id))
        # downloaded files have the order number inserted
        # MOD06_L2.A2018001.0950.061.2018003205209.hdf
        # ->
        # MOD06_L2.A2018001.0950.061.2018003205209.psgscs_000501653312.hdf
        filename_glob = f"{filename.stem}.*{filename.suffix}"
        matching_files = list(Path(data_path).glob(filename_glob))
        N_files = len(matching_files)
        if N_files == 0:
            return False
        elif N_files == 1:
            return True
        else:
            return NotImplementedError(matching_files)

    file_ids = [
        file_id for file_id in file_ids if not file_has_been_downloaded(file_id)
    ]

    if len(file_ids) > 0:
        # place order
        order_kwargs = dict(
            satellite=satellite,
            products=products,
            bbox=bbox,
            file_ids=file_ids,
            MODAPS_EMAIL=MODAPS_EMAIL,
        )

        order_hash = dict_to_hash(order_kwargs)
        order_id = order_db.get(order_hash)

        if order_id is not None:
            order_status = modapsClient.getOrderStatus(order_id)
            if order_status == "Removed":
                order_id = None

        if order_id is None:
            # looks like this order hasn't been made before or needs to be
            # made again, let's do it now so we know what files to download
            order_id = _modaps_order(**order_kwargs)
            order_db.set(order_hash, order_id)


def _ensure_datetime_date(d):
    if isinstance(d, datetime.date):
        return d
    elif isinstance(d, str):
        return datetime.datetime.strptime(d, MODAPS_DATE_FORMAT).date()
    elif type(d) == datetime.datetime:
        warnings.warn(
            f"a datetime.datetime object ({d}) was provided, but modaps queries"
            "are only by date so the time component will be ignored"
        )
        return d.date()
    else:
        raise NotImplementedError(d)


def modaps_pipeline(
    start_date,
    end_date,
    bbox,
    collection,
    satellites,
    products,
    data_path="modaps",
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
    data_path = Path(data_path)

    MODAPS_EMAIL = os.environ.get("MODAPS_EMAIL")
    MODAPS_TOKEN = os.environ.get("MODAPS_TOKEN")

    if MODAPS_TOKEN is None or MODAPS_EMAIL is None:
        raise Exception(
            "Please set your NASA MODAPS credentials using the MODAPS_EMAIL and"
            " MODAPS_TOKEN environment variables. You can create a new account for"
            " NASA LAADS DAAC on https://ladsweb.modaps.eosdis.nasa.gov/"
            " and generate a key on https://ladsweb.modaps.eosdis.nasa.gov/profile/#app-keys"
        )

    start_date = _ensure_datetime_date(start_date)
    end_date = _ensure_datetime_date(end_date)

    # hash(query_kwargs) -> file_ids
    query_db = YamlDB(name="modaps_queries", path=data_path)
    # hash(order_kwargs(...file_ids)) -> order_id
    order_db = YamlDB(name="modaps_orders", path=data_path)
    # file_id -> filenames
    file_db = YamlDB(name="modaps_files", path=data_path)

    date_intervals = list(_create_query_date_intervals(start_date, end_date))

    # query for data and then order any that hasn't already been ordered
    for satellite, (_start_date, _end_date) in tqdm(
        list(itertools.product(satellites, date_intervals)), desc="query & order"
    ):
        _modaps_query_and_order(
            satellite,
            _start_date.strftime(MODAPS_DATE_FORMAT),
            _end_date.strftime(MODAPS_DATE_FORMAT),
            bbox,
            collection,
            products,
            MODAPS_EMAIL,
            query_db,
            order_db,
            file_db,
            data_path,
        )

    # fetch any available orders and delete any removed
    modapsClient = ModapsClient()

    processing_orders = []
    for order in tqdm(order_db.keys, "fetch"):
        order_id = order_db.get(order)
        continue

        order_status = modapsClient.getOrderStatus(order_id)

        if order_status in ["Removed", "Canceled"]:
            order_db.remove(order)
        elif order_status == "Available":
            order_data_path = Path(data_path)
            order_data_path.mkdir(exist_ok=True, parents=True)
            filepaths = modapsClient.fetchFilesForOrder(
                order_id=order_id,
                auth_token=MODAPS_TOKEN,
                path=order_data_path,
            )
        elif order_status in [
            "Requesting files",
            "File request completed",
            "New",
            "Post processing",
            "Post processing complete",
            "Running",
        ]:
            processing_orders.append(order_id)
        else:
            raise NotImplementedError(f"{order_id}: {order_status}")

    if len(processing_orders) > 0:
        raise MODAPSOrderProcessingException(
            "Some MODAPS orders are still processing. Please wait a while and"
            " then try running the pipeline again. You can check details on"
            " your orders on https://ladsweb.modaps.eosdis.nasa.gov/search/history"
        )

    # the files downloaded from MODAPS need further processing
    # for example we want a boolean mask for the MODIS cloudmask
    # TODO: implement postprocessing for other files here too
    filepaths = list(data_path.glob("*.hdf"))

    for filepath in tqdm(filepaths, desc="postprocess"):
        if filepath.suffix != ".hdf":
            continue

        if (
            "Cloud_Mask" in filepath.name
            or len(products) == 1
            and products[0] == "Cloud_Mask_1km"
        ):
            filepath_nc = filepath.parent / filepath.name.replace(".hdf", ".nc")
            if filepath_nc.exists():
                continue
            da_cloudmask = read_MODIS_cloud_mask(filepath=filepath)
            da_cloudmask.to_netcdf(filepath_nc)
        else:
            raise NotImplementedError(filepath)

    filepaths_nc = list(data_path.glob("*.nc"))

    return CloudmetricPipeline(source_files=filepaths_nc)
