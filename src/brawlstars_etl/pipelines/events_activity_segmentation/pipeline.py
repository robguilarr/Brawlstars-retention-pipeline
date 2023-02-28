"""
This is a boilerplate pipeline 'events_activity_segmentation'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import battlelogs_deconstructor, activity_transformer

def create_pipeline(**kwargs) -> Pipeline:
    events_activity_segmentation = pipeline(
        [
            node(
                func=battlelogs_deconstructor,
                inputs=['battlelogs_filtered_data@pyspark','params:events_activity_segmentation'],
                outputs=['event_solo_data@pyspark', 'event_duo_data@pyspark',
                         'event_3v3_data@pyspark', 'event_special_data@pyspark'],
                name='battlelogs_deconstructor_node'
            ),
            node(
                func= activity_transformer,
                inputs=['battlelogs_filtered_data@pyspark', 'params:events_activity_segmentation'],
                outputs= 'user_activity@pyspark',
                name='activity_transformer_node'
            )

        ]
    )

    return events_activity_segmentation