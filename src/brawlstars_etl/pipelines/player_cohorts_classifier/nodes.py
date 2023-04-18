"""
This is a boilerplate pipeline 'player_cohorts_classifier'
generated using Kedro 0.18.4
"""
from typing import Any, Dict
import logging
import pandas as pd
from sklearn.preprocessing import MaxAbsScaler
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.compose import ColumnTransformer

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

    # Check if the length of 'y' matches the transformed data and concatenate them
    try:
        assert len(y) == len(metadata_scaled)
        metadata_scaled = pd.concat([y, metadata_scaled], axis=1)
    except AssertionError:
        log.info("Scaler instance or data is corrupted, try debugging your input data")

    # Return the scaled DataFrame
    return metadata_scaled


def feature_selector(metadata_scaled: pd.DataFrame,
                      parameters: Dict[str, Any]
) -> pd.DataFrame:
    """
    Select top K features from a scaled metadata dataframe.
    Args:
        metadata_scaled: The input metadata as a Pandas DataFrame, with player ID in
        one column and feature values in the remaining columns.
        parameters: A dictionary of input parameters, containing the number of top
        features to select.
    Returns:
        A new metadata dataframe containing only the selected top K features,
        with the same player ID column as the input dataframe.
    """
    # Drop the player ID column from the metadata dataframe
    X_scaled = metadata_scaled.drop('player_id', axis=1)
    # Extract the player ID column from the metadata dataframe
    y = metadata_scaled['player_id']

    # Use SelectKBest and ColumnTransformer to select the top K features
    selector = SelectKBest(f_classif, k=parameters['top_features'])
    preprocessor = ColumnTransformer(transformers=[
        ('SKB_transformer', selector, X_scaled.columns)
    ])
    preprocessor.fit(X_scaled, y)
    # Extract the feature-selected data from the preprocessor object
    X_features = preprocessor.transform(X_scaled)

    # Get the index numbers and names of the selected columns
    new_columns_index = preprocessor.named_transformers_['SKB_transformer'].get_support(
        indices=True)
    new_columns_names = X_scaled.columns[new_columns_index]
    log.info(f"Columns selected: {new_columns_names}")

    # Create a new dataframe with only the selected columns
    X_features = pd.DataFrame(X_features, columns=new_columns_names)
    metadata_reduced = pd.concat([y, X_features], axis=1)

    return metadata_reduced