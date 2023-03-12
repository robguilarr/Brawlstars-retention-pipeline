"""
This is a boilerplate pipeline 'metadata_request_preprocess'
generated using Kedro 0.18.4
"""
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import players_info_request, metadata_preparation

def create_pipeline(**kwargs) -> Pipeline:
    metadata_request_preprocess = pipeline(
        [
            node(
                func=players_info_request,
                inputs=['player_tags_txt','params:player_metadata_request'],
                outputs='player_metadata@pandas',
                name='player_metadata_request_node'
            ),
            node(
                func=metadata_preparation,
                inputs=['player_metadata@pandas', 'params:metadata_preparation'],
                outputs='metadata_prepared@pyspark',
                name='metadata_preparation_node'
            )
        ]
    )
    return metadata_request_preprocess