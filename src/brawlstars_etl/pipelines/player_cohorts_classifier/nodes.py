"""
This is a boilerplate pipeline 'player_cohorts_classifier'
generated using Kedro 0.18.4
"""
from typing import Any, Dict, Tuple, List, Union
import logging
import pandas as pd
import numpy as np
import random
import plotly.graph_objs as go
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
) -> Tuple[Union[bytes, None], Dict[str, Any], Dict[str, Any], go.Figure]:
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
        inertia_plot: Inertia plot for a clustering grid search
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
    plot_dimensions = parameters.get("plot_dimensions")

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

    # Retrieve parameters used to define the best estimator
    best_params_KMeans, eval_params_KMeans = _estimator_param_export(
        eval_params_KMeans, grid_search, seed
    )

    # Return inertia plot to visualize the best estimator
    inertia_plot = _inertia_plot_gen(
        grid_search, plot_dimensions, starting_point_method
    )

    return kmeans_estimator, best_params_KMeans, eval_params_KMeans, inertia_plot


def _estimator_param_export(
    eval_params_KMeans: Dict[str, Any], grid_search: Union[bytes, None], seed: int
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Helper function to extract the best parameters from a KMeans estimator and
    compute its mean negative inertia score."""
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
    return best_params_KMeans, eval_params_KMeans


def _inertia_plot_gen(
    grid_search: Union[bytes, None],
    plot_dimensions: Dict[str, Any],
    starting_point_method: List,
) -> go.Figure:
    """Helper function to generate an inertia plot for a clustering grid search."""
    # Get inertia values for each estimator and store them in a list
    inertia_values = [-score for score in grid_search.cv_results_["mean_test_score"]]
    log.info("Inertia values of all estimators: %s", inertia_values)

    # Get number of clusters used in each estimator and store them in a list
    n_clusters_values = [
        params["n_clusters"] for params in grid_search.cv_results_["params"]
    ]
    log.info("Number of clusters used in each estimator: %s", n_clusters_values)

    # Check if starting_point_method has more than one value
    if len(starting_point_method) > 1:
        log.info("Plot is only applicable to one starting point method")
        fig = _dummy_plot(plot_dimensions)
        return fig
    else:
        # Create a scatter (with lines) plot of ks vs inertias
        fig = go.Figure(
            go.Scatter(x=n_clusters_values, y=inertia_values, mode="lines+markers")
        )
        fig.update_layout(
            xaxis_title="number of clusters, k",
            yaxis_title="inertia",
            xaxis=dict(tickmode="array", tickvals=n_clusters_values),
            width=plot_dimensions.get("width"),
            height=plot_dimensions.get("height"),
        )
        return fig


def _dummy_plot(plot_dimensions: Dict[str, Dict[str, Any]]) -> go.Figure:
    """Helper function to create a dummy chart when the requirements are not met."""
    fig = go.Figure()
    fig.add_annotation(text="Plot not available", showarrow=False, font=dict(size=20))
    fig.update_layout(
        width=plot_dimensions.get("width"), height=plot_dimensions.get("height")
    )
    return fig


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


def centroid_plot_generator(
    metadata_reduced: pd.DataFrame,
    kmeans_estimator: Union[bytes, None],
    parameters: Dict[str, Any],
) -> Tuple[go.Figure, go.Figure, go.Figure, go.Figure]:
    """
    Generate scatter plot figures for all possible pairs of features using the
    input metadata features (reduced, features selected), highlighting the centroids
    of a KMeans clustering estimator with the best hyperparameters found.
    Args:
        metadata_reduced: DataFrame containing the reduced metadata to be
        used for plotting
        kmeans_estimator: The KMeans clustering model to be used to generate the
        centroids
        parameters: A dictionary of parameters used to configure the plot dimensions
    Returns:
        Plot figures, where each plot shows a different pair of features in the input
        dataset, highlighting the centroids of the KMeans clustering model with the best
        hyperparameters found.
    """
    # Extract dimension parameters for plotting
    plot_dimensions = parameters.get("plot_dimensions")

    # Drop the 'player_id' column from the 'metadata_reduced' dataframe
    X_features = metadata_reduced.drop(columns=["player_id"])

    # Create a dictionary of feature names and their index in the feature matrix
    features = {
        X_features.columns[feature_number]: feature_number
        for feature_number in range(len(X_features.columns))
    }

    # Generate a list of all possible feature pairs
    feature_pairs = [
        (i, j) for i in range(len(features)) for j in range(i + 1, len(features))
    ]

    # Create an empty dictionary to store the plot figures
    plot_figures = {}

    # Iterate over each feature pair and create a plot figure for each pair
    for col_x, col_y in feature_pairs:
        log.info(f"Generating plot for features: {col_x} vs {col_y}")
        # Add some noise to the feature matrix
        noise = np.random.normal(loc=0, scale=0.05, size=X_features.shape)

        # Fit a KMeans model with the best hyperparameters found
        kmeans_estimator.fit(X_features + noise)

        # Get the coordinates of the centroids
        centroids = kmeans_estimator.cluster_centers_
        centroids_x = centroids[:, col_x]
        centroids_y = centroids[:, col_y]

        # Get the names of the two features being plotted
        feature_x = list(features.keys())[col_x]
        feature_y = list(features.keys())[col_y]

        # Get the values of the two features being plotted
        xs = X_features[feature_x]
        ys = X_features[feature_y]

        # Get the cluster labels for each data point
        labels = kmeans_estimator.labels_

        # Generate a palette of colors for the clusters
        colors = _generate_palette(kmeans_estimator.n_clusters)

        # Assign a color to each data point based on its cluster label
        point_colors = [colors[label % len(colors)] for label in labels]

        # Create a plot figure with two scatter traces: one for the data points and one
        # for the centroids
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=xs, y=ys, mode="markers", marker=dict(color=point_colors, size=5)
            )
        )
        fig.add_trace(
            go.Scatter(
                x=centroids_x,
                y=centroids_y,
                mode="markers",
                marker=dict(
                    symbol="diamond",
                    size=10,
                    color=colors,
                    line=dict(color="black", width=2),
                ),
            )
        )

        # Add axis labels to the plot
        fig.update_layout(
            xaxis_title=f"{feature_x} scaled", yaxis_title=f"{feature_y} scaled"
        )

        # Set the size of the plot figure
        fig.update_layout(
            width=plot_dimensions.get("width"), height=plot_dimensions.get("height")
        )

        # Hide the legend in the plot
        fig.update_layout(showlegend=False)

        # Set the name of the plot figure and store it in the dictionary
        plot_name = f"{feature_x}_vs_{feature_y}"
        plot_figures[plot_name] = fig

    # Retrieve list of plot names as list
    plot_figures_names = list(plot_figures.keys())

    try:
        assert len(plot_figures_names) >= 4
        return (
            plot_figures[plot_figures_names[0]],
            plot_figures[plot_figures_names[1]],
            plot_figures[plot_figures_names[2]],
            plot_figures[plot_figures_names[3]],
        )
    except AssertionError:
        log.info("To save the plot figures the minimum number of plots must be 4")
        fig = _dummy_plot(plot_dimensions)
        return fig, fig, fig, fig


def _generate_palette(num_colors):
    """Helper function to generate a list of hexadecimal color codes, where each code
    consists of a '#' followed by 6 randomly generated characters (0-9 and A-F)"""
    # Generate 'num_colors' number of color codes
    return [
        "#" + "".join([random.choice("0123456789ABCDEF") for j in range(6)])
        for i in range(num_colors)
    ]
