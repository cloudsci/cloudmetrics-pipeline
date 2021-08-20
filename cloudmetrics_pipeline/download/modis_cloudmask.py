from pyhdf.SD import SD
import numpy as np
import xarray as xr


QUALTITY_OPTIONS = ["confident_cloudy", "probably_cloudy"]


def read_bits(bit_start, bit_count, value):
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

    fh = SD(str(filepath))

    field_name = list(fh.datasets().keys())[0]

    field_values = fh.select(field_name).get()[..., 0]

    # the quality of the field mask is defined by the first two bits
    field_quality_values = read_bits(1, 2, field_values)

    cloud_mask = field_quality_values <= quality_threshold

    da = xr.DataArray(cloud_mask, attrs=dict(resolution="1km"))

    return da
