from pathlib import Path


import cloudmetrics_pipeline as cm_pipeline


def test_MODIS_greyscale_metrics():
    data_path = Path("/tmp/cm_pipeline")
    data_path.mkdir(exist_ok=True, parents=True)

    cm_pipeline.download.modis.downloadMODISImgs(
        startDate="2002-12-01",
        endDate="2002-12-02",
        extent=[-58, -48, 10, 20],
        savePath=str(data_path),
        exist_skip=True,
    )

    filepaths = list(data_path.glob("*.jpeg"))

    (
        cm_pipeline.find_scenes(source_files=filepaths)
        .mask(fn=cm_pipeline.masks.rgb_greyscale_mask)
        .compute_metrics(metrics=["fractal_dimension"])
        .execute()
    )
