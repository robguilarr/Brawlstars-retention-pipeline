"""
This is a boilerplate pipeline 'player_cohorts_classifier'
generated using Kedro 0.18.4
"""
from typing import Any, Dict, Tuple, List, Union
import logging
import pandas as pd
from sklearn.preprocessing import MaxAbsScaler
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.compose import ColumnTransformer
from sklearn.cluster import KMeans
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import (
    silhouette_score,
    davies_bouldin_score,
    calinski_harabasz_score,
)

log = logging.getLogger(__name__)


def feature_scaler(metadata_prepared: pd.DataFrame) -> pd.DataFrame:
    """
    Applies MaxAbsScaler on the input data and returns the transformed data.
    Args:
        metadata_prepared: Dataframe containing player metadata with 'player_id'
        column and numeric features.
    Returns:
        Dataframe with scaled numeric features
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
) -> Tuple[pd.DataFrame, Dict[str, List]]:
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
        with the same player ID column as the input dataframe
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

    return metadata_reduced, {"features_selected": list(new_columns_names)}


def kmeans_estimator_grid_search(
    metadata_reduced: pd.DataFrame, parameters: Dict[str, Any]
) -> Tuple[Union[bytes, None], Dict[str, Any], Dict[str, Any]]:
    """
    Perform a grid search with cross-validation to find the best hyperparameters for
    KMeans clustering algorithm.
    Args:
        metadata_reduced: The metadata of players to be clustered.
        parameters: The parameters for the grid search of the estimator.
    Returns:
        kmeans_estimator: Pickle file containing the KMeans estimator
        best_params_KMeans: The best parameters found by GridSearchCV
        eval_params_KMeans: The parameters evaluated by GridSearchCV
    """
    # Drop the 'player_id' column from the 'metadata_reduced' dataframe
    X_features = metadata_reduced.drop(columns=["player_id"])

    # Define hyperparameters for KMeans algorithm
    seed = parameters.get("random_state")
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
    log.info(
        f"Performing GridSearch Cross-Validation for KMeans instance\n"
        f"** Parameters in use **: {eval_params_KMeans}"
    )
    grid_search = GridSearchCV(
        KMeans(random_state=seed), param_grid=eval_params_KMeans, cv=cv, n_jobs=cores
    )
    grid_search.fit(X_features)

    # Extract best estimator
    kmeans_estimator = grid_search.best_estimator_

    # Extract the best estimator parameters
    best_params_KMeans = {
        str(key): str(value) for key, value in grid_search.best_params_.items()
    }
    log.info(f"Best Parameters: {best_params_KMeans}")

    # Compute mean negative inertia score for all the estimators
    mean_neg_inertia = grid_search.best_score_
    log.info(f"Best Mean Inertia Score: {mean_neg_inertia:.2f}")

    # Convert to JSON-serializable formats
    best_params_KMeans = {
        str(key): str(value) for key, value in best_params_KMeans.items()
    }
    eval_params_KMeans = {
        str(key): str(value) for key, value in eval_params_KMeans.items()
    }
    eval_params_KMeans["random_state"] = seed

    return kmeans_estimator, best_params_KMeans, eval_params_KMeans


def kmeans_inference(
    metadata_reduced: pd.DataFrame, kmeans_estimator: Union[bytes, None]
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Cluster players' metadata using K-Means algorithm and compute clustering
    evaluation metrics.
    The following clustering evaluation metrics are computed and logged:
        - Total Inertia Score
        - Silhouette Score
        - Davies-Bouldin Score
        - Calinski-Harabasz Score
    Args:
        metadata_reduced: DataFrame containing players' metadata to cluster.
        kmeans_estimator: Trained K-Means estimator (model).

    Returns:
        Clustered players' metadata and their corresponding cluster labels and a
        dictionary (metrics_KMeans) containing clustering evaluation metrics computed
    """
    # Drop the 'player_id' column from the 'metadata_reduced' dataframe
    X_features = metadata_reduced.drop(columns=["player_id"])

    # Extract the 'player_id' column from the 'metadata_reduced' dataframe
    y = metadata_reduced["player_id"]

    # Generate and append labels
    predicted_labels = kmeans_estimator.predict(X_features)
    player_metadata_clustered = pd.concat(
        [y, pd.Series(predicted_labels, name="cluster_labels"), X_features], axis=1
    )

    # Extract total inertia score
    inertia_score = kmeans_estimator.inertia_
    log.info(f"Total Inertia Score: {inertia_score:.2f}")

    # Extract Silhouette score
    silhouette = silhouette_score(X_features, predicted_labels)
    log.info(f"Silhouette Score: {silhouette:.2f}")

    # Extract Davies-Bouldin index
    davies_bouldin = davies_bouldin_score(X_features, predicted_labels)
    log.info(f"Davies-Bouldin Score: {davies_bouldin:.2f}")

    # Extract Calinski-Harabasz index
    calinski_harabasz = calinski_harabasz_score(X_features, predicted_labels)
    log.info(f"Calinski-Harabasz Score: {calinski_harabasz:.2f}")

    # Save metrics for tracking
    metrics_KMeans = {
        "silhouette_score": silhouette,
        "davies_bouldin_score": davies_bouldin,
        "calinski_harabasz_score": calinski_harabasz,
        "total_inertia": inertia_score,
    }
    metrics_KMeans = {str(key): value for key, value in metrics_KMeans.items()}

    return player_metadata_clustered, metrics_KMeans
