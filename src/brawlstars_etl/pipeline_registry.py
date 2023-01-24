"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from brawlstars_etl.pipelines import metadata_request_preprocess
from brawlstars_etl.pipelines import battlelogs_request_preprocess

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # Call pipelines from source directory
    brp = battlelogs_request_preprocess.create_pipeline()
    mrp = metadata_request_preprocess.create_pipeline()


    return {"__default__": brp, # + mrp
            "battlelogs_request_preprocess": brp,
            "metadata_request_preprocess": mrp}
