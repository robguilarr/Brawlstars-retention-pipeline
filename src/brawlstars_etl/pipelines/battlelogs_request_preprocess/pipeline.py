"""
This is a boilerplate pipeline 'battlelogs_request_preprocess'
generated using Kedro 0.18.4
"""
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import battlelogs_request
from .nodes import battlelogs_filter

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=battlelogs_request,
                inputs="player_tags",
                outputs="raw_battlelogs@Pandas",
                name="battlelogs_request_node"
            ),
            node(
                func=battlelogs_filter,
                inputs=['raw_battlelogs@Pandas','parameters'],
                outputs='master_event_data@Spark',
                name='battlelogs_filter_node'
            )
        ],
        namespace= 'battlelogs_request_and_preprocess',
        inputs= ['player_tags'],
        outputs= 'raw_battlelogs@Pandas'
    )
