import logging
from typing import List, Optional

import numpy as np
import rasterio as rio
from rasterio import  MemoryFile
from rasterio.enums import Resampling

logger = logging.getLogger(__name__)

def is_bigtiff(data: np.ndarray or np.ma.MaskedArray or str):
    """
    Check if array is larger than 4 GB. GDAL BIGTIFF=IF_SAFER/IF_NEEDED isn't reliable when used
    with compression. Instead, use this to estimate if uncompressed file is > 4GB, and then YES/NO.

    :param data:
        Numpy array or masked array containing raster data.
    :return:
        True if array qualifies as BIGTIFF.
    """
    if isinstance(data, str):
        with rio.open(data) as src:
            data = src.read()

    data_size = data.size * data.itemsize
    if data_size > 4000000000:  # 4 GB
        return True
    else:
        return False


def render_cog(
    data: np.ndarray or np.ma.masked_array,
    profile: dict,
    compression: str = "lzw",
    overview_resampling: Resampling = Resampling.nearest,
    overview_levels: List[int] = [2, 4, 8, 16, 32],
    nodata: Optional[float] = None,
    colormap: Optional[str] = None,
    colormap_band: int = 1,
) -> None:
    """
    Write array to Cloud Optimized GeoTIFF (COG) memory file and return byte stream.
    :param data:
        Numpy array or masked array (where mask == nodata) to be saved to COG.
    :param profile:
        Rasterio dataset profile.
    :param compression:
        Type of compression to apply to data.
    :param overview_resampling:
        Resampling method to apply when building overviews (will affect visual quality when zoomed out).
    :param overview_levels:
        Decimation levels at which to build overviews
    :param nodata:
        Nodata value that should be used to mask data. Will overwrite value in raster profile.
        Required if nodata is not set in raster profile.
    :param validate:
        Set True to ensure the output COG is valid.
    :param colormap:
        Colormap, matching format here: https://rasterio.readthedocs.io/en/latest/topics/color.html#writing-colormaps
        to write to raster metadata (data must be 8 bit integer if colormap is set).
    :param colormap_band:
        Optional. Band to apply colormap to, if using.
    :return
        Byte stream from COG memory file.
    """
    
    # Fill masked values with nodata to ensure nodata values are properly written.
    if np.ma.is_masked(data) and nodata:
        data = data.filled(nodata)

    # Check that it is 3d. If 2d add 3rd dimension.
    if len(data.shape) == 2:
        data = np.expand_dims(data, 0)

    if is_bigtiff(data):
        bigtiff = "YES"
    else:
        bigtiff = "NO"

    profile.update(
        driver="COG",
        nodata=nodata,
        dtype=data.dtype,
        count=data.shape[0],
        tiled=True,
        compress=compression,
        BIGTIFF=bigtiff,
        COPY_SRC_OVERVIEWS="YES",
    )

    with MemoryFile() as memfile:
        with memfile.open(**profile) as dst:
            dst.write(data)
            if colormap:
                dst.write_colormap(colormap_band, colormap)
            dst.build_overviews(overview_levels, overview_resampling)

        return memfile.read()