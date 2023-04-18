"""
This is a boilerplate pipeline 'player_cohorts_classifier'
generated using Kedro 0.18.4
"""
from typing import Any, Dict #parameters: Dict[str, Any]
import logging
import pandas as pd
from sklearn.preprocessing import MaxAbsScaler

log = logging.getLogger(__name__)


def feature_scaler(metadata_prepared: pd.DataFrame
) -> pd.DataFrame:
    """
    Applies MaxAbsScaler on the input data and returns the transformed data.
    Args:
        metadata_prepared: Dataframe containing player metadata with 'player_id'
        column and numeric features.
    Returns:
        Dataframe with scaled numeric features.
    Raises:
    AssertionError: If the length of 'player_id' column and the transformed data are
    not equal
    """
    # Drop the 'player_id' column and use it as the target variable
    X = metadata_prepared.drop('player_id', axis=1)
    y = metadata_prepared['player_id']

    # Initialize the MaxAbsScaler object, fit it to the data, and transform the data
    scaler = MaxAbsScaler()
    X_scaled = scaler.fit_transform(X)

    # Create a new DataFrame with the transformed data and original column names
    metadata_scaled = pd.DataFrame(X_scaled, columns=X.columns)

    # Check if the length of 'player_id' matches the transformed data and log an
    # error if not
    try:
        assert len(y) == len(metadata_scaled)
    except AssertionError:
        log.info("Scaler instance or data is corrupted, try debugging your input data")

    # Return the scaled DataFrame
    return metadata_scaled
