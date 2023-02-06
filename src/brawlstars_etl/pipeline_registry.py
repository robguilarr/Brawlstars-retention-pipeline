"""Project pipelines."""
from typing import Dict
# from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from brawlstars_etl.pipelines import battlelogs_request_preprocess
from brawlstars_etl.pipelines import events_activity_segmentation
from brawlstars_etl.pipelines import metadata_request_preprocess
from brawlstars_etl.pipelines import validation_data_assets
from brawlstars_etl.pipelines import cloud_data_export

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # Call pipelines from source directory
    brp = battlelogs_request_preprocess.create_pipeline()
    mrp = metadata_request_preprocess.create_pipeline()
    eas = events_activity_segmentation.create_pipeline()
    vda = validation_data_assets.create_pipeline()
    cde = cloud_data_export.create_pipeline()

    return {"__default__": brp + eas + mrp, #  + vda
            "battlelogs_request_preprocess": brp,
            "events_activity_segmentation": eas,
            "metadata_request_preprocess": mrp,
            "validation_data_assets": vda,
            "cloud_data_export": cde
    }