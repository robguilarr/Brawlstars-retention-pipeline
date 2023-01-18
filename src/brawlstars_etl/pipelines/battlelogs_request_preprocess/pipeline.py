"""
This is a boilerplate pipeline 'battlelogs_request_preprocess'
generated using Kedro 0.18.4
"""
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import battlelogs_request

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=battlelogs_request,
                inputs="player_tags",
                outputs="raw_battlelogs@Pandas",
                name="battlelogs_request_node"
            )
        ]
    )
