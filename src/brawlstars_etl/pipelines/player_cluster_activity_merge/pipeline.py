"""
This is a boilerplate pipeline 'events_activity_segmentation'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import player_cluster_activity_concatenator, user_retention_plot_gen


def create_pipeline(**kwargs) -> Pipeline:
    namespace = "Player Activity Clustering & Merge"
    player_cluster_activity_merge = pipeline(
        [
            node(
                func=player_cluster_activity_concatenator,
                inputs=[
                    "user_activity_data@pyspark",
                    "players_metadata_clustered@pandas",
                    "params:ratio_register",
                    "params:activity_transformer",
                ],
                outputs="player_clustered_activity@pandas",
                name="player_cluster_activity_concatenator_node",
                namespace=namespace,
            ),
            node(
                func=user_retention_plot_gen,
                inputs=[
                    "player_clustered_activity@pandas",
                    "params:user_retention_plot_gen",
                ],
                outputs="user_retention_plot",
                name="user_retention_plot_gen_node",
                namespace=namespace,
            ),
        ]
    )
    return player_cluster_activity_merge
