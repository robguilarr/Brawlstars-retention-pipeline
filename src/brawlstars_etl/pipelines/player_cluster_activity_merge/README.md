# Pipeline - Player Cluster Activity Merge

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.18.4`.

## Overview

This is pipeline is composed of two nodes, `player_cluster_activity_concatenator` and 
`user_retention_plot_gen`, designed to generate a user retention plot given player 
activity data.

The `player_cluster_activity_concatenator` node takes as input a Spark DataFrame 
containing user retention data, a Pandas DataFrame containing player data and 
cluster labels, a dictionary with the retention ratio's parameters, and a dictionary 
with activity transformation parameters. It outputs a Pandas DataFrame containing 
concatenated player cluster activity data and retention ratios.

The `user_retention_plot_gen` node takes as input the Pandas DataFrame produced by the 
`player_cluster_activity_concatenator` node and a dictionary containing plot format 
parameters. It outputs a Plotly figure representing the user retention plot.
