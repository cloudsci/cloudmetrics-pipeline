import worldview_dl


def download_rgb_image(
    filepath, time, bbox, satellite, resolution=0.01, image_format="png"
):
    # NB: worlview_dl uses SWNE for the bounding-box rather than WESN
    bbox_ = [bbox[2], bbox[0], bbox[3], bbox[1]]
    if satellite == "MODIS_Aqua" or satellite == "MODIS_Terra":
        layers = [f"{satellite}_CorrectedReflectance_TrueColor"]
    else:
        raise NotImplementedError(satellite)
    worldview_dl.download_image(
        fn=filepath,
        time=time,
        bbox=bbox_,
        layers=layers,
        image_format=image_format,
        resolution=resolution,
    )
