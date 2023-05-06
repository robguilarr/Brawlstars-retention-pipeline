# Pipeline - Player Cohorts Classifier

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.18.4`.

## Overview

This pipeline consists of five nodes: `feature_scaler`, `feature_selector`, 
`kmeans_estimator_grid_search`, `kmeans_inference`, and `centroid_plot_generator`.

Together, the pipelines apply feature scaling, feature selection, hyperparameter 
tuning, clustering, and visualization using the clusters of a KMeans algorithm.

The core inputs are: a `metadata_prepared` dataframe in the `feature_scaler` node; the 
`metadata_scaled` dataframe, and parameters dictionary containing the number of top 
features to select in the `feature_selector` node; and `metadata_reduced` dataframe 
and parameters dictionary containing KMeans algorithm hyperparameters in the 
`kmeans_estimator_grid_search` node.

The pipeline outputs are a `metadata_scaled` dataframe in the `feature_scaler` node; a 
`metadata_reduced` dataframe and a dictionary of features_selected list in the 
`feature_selector` node; a `kmeans_estimator` pickle file, the best and evaluated 
KMeans algorithm hyperparameters, and a KMeans algorithm inertia plot in the 
`kmeans_estimator_grid_search` node; and a Plotly scatter plot of the KMeans algorithm 
centroids in the `centroid_plot_generator` node.
