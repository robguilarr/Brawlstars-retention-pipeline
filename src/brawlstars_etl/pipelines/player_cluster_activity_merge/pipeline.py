"""
This is a boilerplate pipeline 'events_activity_segmentation'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import player_cluster_activity_concatenator


def create_pipeline(**kwargs) -> Pipeline:
    player_cluster_activity_merge = pipeline(
        [
            node(
                func=player_cluster_activity_concatenator,
                inputs=[
                    "user_activity_data@pyspark",
                    "players_metadata_clustered@pandas",
                    "params:ratio_register",
                    "params:activity_transformer"
                ],
                outputs="player_clustered_activity@pandas",
                name="player_cluster_activity_concatenator_node",
            ),
        ]
    )
    return player_cluster_activity_merge
