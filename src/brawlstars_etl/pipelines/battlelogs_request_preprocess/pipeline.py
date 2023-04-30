"""
This is a boilerplate pipeline 'battlelogs_request_preprocess'
generated using Kedro 0.18.4
"""
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import battlelogs_request, battlelogs_filter


def create_pipeline(**kwargs) -> Pipeline:
    namespace = "Battlelogs Request & Preprocess"
    battlelogs_request_preprocess = pipeline(
        [
            node(
                func=battlelogs_request,
                inputs=["player_tags_txt", "params:battlelogs_request"],
                outputs="raw_battlelogs_data@pandas",
                name="battlelogs_request_node",
                namespace=namespace,
            ),
            node(
                func=battlelogs_filter,
                inputs=["raw_battlelogs_data@pandas", "params:battlelogs_filter"],
                outputs="battlelogs_filtered_data@pyspark",
                name="battlelogs_filter_node",
                namespace=namespace,
            ),
        ]
    )

    return battlelogs_request_preprocess
