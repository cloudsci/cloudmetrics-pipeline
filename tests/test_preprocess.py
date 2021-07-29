from pathlib import Path


import cloudmetrics_pipeline as cm_pipeline


def test_sceneids_images():
    data_path = Path("/tmp/cm_pipeline")
    data_path.mkdir(exist_ok=True, parents=True)

    cm_pipeline.download.modis.downloadMODISImgs(
        startDate="2002-12-01",
        endDate="2002-12-02",
        extent=[-58, -48, 10, 20],
        savePath=str(data_path),
    )

    cm_pipeline.find_scenes.produce_scene_ids(data_path=data_path)
