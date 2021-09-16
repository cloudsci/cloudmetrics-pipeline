"""
Used resources:

- https://gis.stackexchange.com/questions/328535/opening-eos-netcdf4-hdf5-file-with-correct-format-using-xarray
- https://github.com/pytroll/satpy/blob/e72a3f029e6a94b55f48d894cbad20b1f4ef0562/satpy/etc/readers/modis_l2.yaml
- https://github.com/mapbox/rasterio/issues/2026#issuecomment-720125978
"""
from pyhdf.SD import SD
import numpy as np
import xarray as xr
import rioxarray as rxr
import parse
import datetime


QUALTITY_OPTIONS = ["confident_cloudy", "probably_cloudy"]

# MOD06_L2.A2020001.mosaic.061.2021258091607.psmcgscs_000501652987.Cloud_Mask_1km.hdf
FILENAME_FORMAT = "MOD06_L2.A{acquisition_date}.mosaic.061.{timestamp}.psmcgscs_{order_id}.{product}.hdf"


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

    da = rxr.open_rasterio(filepath)
    assert "MODIS Cloud Mask" in da.attrs.get("long_name")

    # the `x` and `y` coordinates are lon and lat respectively
    da = da.rename(dict(x="lon", y="lat"))

    # the first band contains the quality measure in the first two bits
    da_field_quality_values = read_bits(da.sel(band=1), 1, 2)
    da_cloud_mask = da_field_quality_values <= quality_threshold

    file_meta = parse.parse(FILENAME_FORMAT, filepath.name)
    acquisition_date = datetime.datetime.strptime(file_meta["acquisition_date"], "%Y%j")
    da_cloud_mask["time"] = acquisition_date
    da_cloud_mask.name = "cloud_mask"
    da_cloud_mask.attrs["type"] = quality_flag

    return da_cloud_mask
