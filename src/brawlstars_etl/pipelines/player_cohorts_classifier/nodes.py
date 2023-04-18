"""
This is a boilerplate pipeline 'player_cohorts_classifier'
generated using Kedro 0.18.4
"""
from typing import Any, Dict, Tuple
import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import MaxAbsScaler
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.compose import ColumnTransformer
from sklearn.cluster import KMeans
from sklearn.model_selection import GridSearchCV

log = logging.getLogger(__name__)


def feature_scaler(metadata_prepared: pd.DataFrame) -> pd.DataFrame:
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
    X = metadata_prepared.drop("player_id", axis=1)
    y = metadata_prepared["player_id"]

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


def feature_selector(
    metadata_scaled: pd.DataFrame, parameters: Dict[str, Any]
) -> pd.DataFrame:
    """
    Select top K features from a scaled metadata dataframe. Here the module select
    n_features_to_select to consider the selection of features to represent:
        - Player capacity of earning credits (representation of trophies).
        - Capacity of being a team player (representation of 3v3 or Duo victories).
        - Solo skills (representation of Solo victories).
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
    X_scaled = metadata_scaled.drop("player_id", axis=1)
    # Extract the player ID column from the metadata dataframe
    y = metadata_scaled["player_id"]

    # Use SelectKBest and ColumnTransformer to select the top K features
    selector = SelectKBest(f_classif, k=parameters["top_features"])
    preprocessor = ColumnTransformer(
        transformers=[("SKB_transformer", selector, X_scaled.columns)]
    )
    preprocessor.fit(X_scaled, y)
    # Extract the feature-selected data from the preprocessor object
    X_features = preprocessor.transform(X_scaled)

    # Get the index numbers and names of the selected columns
    new_columns_index = preprocessor.named_transformers_["SKB_transformer"].get_support(
        indices=True
    )
    new_columns_names = X_scaled.columns[new_columns_index]
    log.info(f"Columns selected: {new_columns_names}")

    # Create a new dataframe with only the selected columns
    X_features = pd.DataFrame(X_features, columns=new_columns_names)
    metadata_reduced = pd.concat([y, X_features], axis=1)

    return metadata_reduced


def players_clustering(
    metadata_reduced: pd.DataFrame, parameters: Dict[str, Any]
) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Cluster players using KMeans, a partition-based clustering algorithm that groups
    data points into a pre-defined number of clusters. It starts by selecting random
    centroids (cluster centers) and then iteratively assigns each data point to the
    nearest centroid based on the distance metric. After all the points are assigned,
    the centroids are updated by taking the mean of all the points in the cluster.
    The algorithm repeats the process until the centroids no longer change, and the
    clusters become stable.
    Args:
        metadata_reduced: The metadata of players to be clustered.
        parameters: The parameters for the clustering.
    Returns:
        - players_clustered: A DataFrame containing the clustered data.
        - best_params_KMeans: The best parameters found by GridSearchCV.
        - eval_params_KMeans: The parameters evaluated by GridSearchCV.
        - metrics_KMeans: The clustering metrics.
    """
    # Drop the 'player_id' column from the 'metadata_reduced' dataframe
    X_features = metadata_reduced.drop(columns=["player_id"])

    # Extract the 'player_id' column from the 'metadata_reduced' dataframe
    y = metadata_reduced["player_id"]

    # Define hyperparameters for KMeans algorithm
    max_n_cluster = parameters.get("max_n_cluster")
    starting_point_method = parameters.get("starting_point_method", "random")
    max_iter = parameters.get("max_iter")
    distortion_tol = parameters.get("distortion_tol")
    cv = parameters.get("cross_validations")

    # Set hyperparameters for GridSearchCV
    eval_params_KMeans = {
        "n_clusters": range(2, max_n_cluster),
        "init": starting_point_method,
        "max_iter": max_iter,
        "tol": distortion_tol,
    }

    # Set number of CPUs to use
    cores = parameters.get("cores", -1)

    # Perform GridSearch Cross-Validation for KMeans instance
    log.info("Performing GridSearch Cross-Validation for KMeans instance")
    log.info(f"** Parameters in use **: {eval_params_KMeans}")
    grid_search = GridSearchCV(
        KMeans(random_state=42), param_grid=eval_params_KMeans, cv=cv, n_jobs=cores
    )
    grid_search.fit(X_features)

    # Extract best estimator
    best_kmeans = grid_search.best_estimator_

    # Generate and append labels
    predicted_labels = best_kmeans.predict(X_features)
    players_clustered = pd.concat([y, pd.Series(predicted_labels), X_features], axis=1)

    # Extract the best estimator parameters
    best_params_KMeans = {
        str(key): str(value) for key, value in grid_search.best_params_.items()
    }
    log.info(f"Best Parameters: {best_params_KMeans}")

    # Compute mean negative inertia score for all the estimators
    mean_neg_inertia = grid_search.best_score_
    log.info(f"Best Mean Inertia Score: {mean_neg_inertia:.2f}")

    # Extract total inertia score
    inertia_score = best_kmeans.inertia_
    log.info(f"Best Total Inertia: {inertia_score:.2f}")

    # Save metrics for tracking
    metrics_KMeans = {
        "mean_neg_inertia": mean_neg_inertia,
        "total_inertia": inertia_score,
    }

    # Convert to JSON-serializable formats
    best_params_KMeans = {
        str(key): str(value) for key, value in best_params_KMeans.items()
    }
    eval_params_KMeans = {
        str(key): str(value) for key, value in eval_params_KMeans.items()
    }
    metrics_KMeans = {
        str(key): str(value) for key, value in metrics_KMeans.items()
    }

    return players_clustered, best_params_KMeans, eval_params_KMeans, metrics_KMeans
