"""
This is a boilerplate pipeline 'player_cohorts_classifier'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import feature_scaler, feature_selector


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
                outputs="metadata_reduced@pandas",
                name="feature_selector_node",
            ),
        ]
    )
    return player_cohorts_classifier
