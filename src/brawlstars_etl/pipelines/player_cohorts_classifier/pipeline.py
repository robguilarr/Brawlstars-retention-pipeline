"""
This is a boilerplate pipeline 'player_cohorts_classifier'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    feature_scaler,
    feature_selector,
    kmeans_estimator_grid_search,
    kmeans_inference,
    centroid_plot_generator,
)


def create_pipeline(**kwargs) -> Pipeline:
    player_cohorts_classifier = pipeline(
        [
            node(
                func=feature_scaler,
                inputs=["metadata_prepared@pandas"],
                outputs="metadata_scaled@pandas",
                name="feature_scaler_node",
            ),
            node(
                func=feature_selector,
                inputs=["metadata_scaled@pandas", "params:feature_selector"],
                outputs=["metadata_reduced@pandas", "features_selected"],
                name="feature_selector_node",
            ),
            node(
                func=kmeans_estimator_grid_search,
                inputs=[
                    "metadata_reduced@pandas",
                    "params:kmeans_estimator_grid_search",
                ],
                outputs=[
                    "kmeans_estimator",
                    "best_params_KMeans",
                    "eval_params_KMeans",
                    "inertia_plot",
                ],
                name="kmeans_estimator_grid_search_node",
            ),
            node(
                func=kmeans_inference,
                inputs=["metadata_reduced@pandas", "kmeans_estimator"],
                outputs=[
                    "players_metadata_clustered@pandas",
                    "metrics_KMeans",
                ],
                name="kmeans_inference_node",
            ),
            node(
                func=centroid_plot_generator,
                inputs=[
                    "metadata_reduced@pandas",
                    "kmeans_estimator",
                    "params:centroid_plot_generator",
                ],
                outputs=[
                    "centroid_plot_1",
                    "centroid_plot_2",
                    "centroid_plot_3",
                    "centroid_plot_4",
                ],
                name="centroid_plot_generator_node",
            ),
        ]
    )
    return player_cohorts_classifier
