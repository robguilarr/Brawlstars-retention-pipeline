"""
This is a boilerplate pipeline 'events_activity_segmentation'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import battlelogs_deconstructor, activity_transformer, ratio_register

def create_pipeline(**kwargs) -> Pipeline:
    events_activity_segmentation = pipeline(
        [
            node(
                func=battlelogs_deconstructor,
                inputs=['battlelogs_filtered_data@pyspark','params:battlelogs_deconstructor'],
                outputs=['event_solo_data@pyspark', 'event_duo_data@pyspark',
                         'event_3v3_data@pyspark', 'event_special_data@pyspark'],
                name='battlelogs_deconstructor_node'
            ),
            node(
                func= activity_transformer,
                inputs=['battlelogs_filtered_data@pyspark', 'params:activity_transformer'],
                outputs= 'user_activity_data@pyspark',
                name='activity_transformer_node'
            ),
            node(
                func=ratio_register,
                inputs=['user_activity_data@pyspark',
                        'params:ratio_register', 'params:activity_transformer'],
                outputs='cohort_activity_data@pyspark',
                name='ratio_register_node'
            )
        ]
    )
    return events_activity_segmentation