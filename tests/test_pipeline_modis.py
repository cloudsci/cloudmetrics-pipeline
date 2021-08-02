from pathlib import Path


import cloudmetrics_pipeline as cm_pipeline


def test_MODIS_greyscale_metrics():
    data_path = Path("/tmp/cm_pipeline")
    data_path.mkdir(exist_ok=True, parents=True)

    da_metric = (
        cm_pipeline.download.modis_rgb(
            start_date="2020-01-01",
            end_date="2020-01-02",
            bbox=[-58, -48, 10, 20],
            data_path=data_path,
        )
        .mask(fn=cm_pipeline.masks.rgb_greyscale_mask)
        .compute_metrics(metrics=["fractal_dimension"])
        .execute()
    )

    assert da_metric.name == "fractal_dimension"
