"""
This is a boilerplate pipeline 'battlelogs_request_preprocess'
generated using Kedro 0.18.4
"""
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import battlelogs_request, battlelogs_filter, battlelogs_deconstructor

def create_pipeline(**kwargs) -> Pipeline:
    pipeline_instance = pipeline(
        [
            node(
                func=battlelogs_request,
                inputs='player_tags_txt',
                outputs='raw_battlelogs_data@pandas',
                name="battlelogs_request_node"
            ),
            node(
                func=battlelogs_filter,
                inputs=['raw_battlelogs_data@pandas','parameters'],
                outputs='battlelogs_filtered_data@pyspark',
                name='battlelogs_filter_node'
            ),
            node(
                func=battlelogs_deconstructor,
                inputs='battlelogs_filtered_data@pyspark',
                outputs= ['events_showdown_data@pyspark','events_special_data@pyspark'],
                name='battlelogs_deconstructor_node'
            )
        ]
    )

    return pipeline_instance