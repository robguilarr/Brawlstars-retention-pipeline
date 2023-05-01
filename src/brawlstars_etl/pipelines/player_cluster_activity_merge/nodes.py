"""
This is a boilerplate pipeline 'player_cluster_activity_merge'
generated using Kedro 0.18.4
"""

import logging
from ast import literal_eval
from typing import Dict, List, Any
import re
import pandas as pd
import plotly.graph_objs as go
import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

log = logging.getLogger(__name__)


def player_cluster_activity_concatenator(
    user_activity_data: pyspark.sql.DataFrame,
    players_metadata_clustered: pd.DataFrame,
    params_ratio_register: Dict[str, Any],
    params_activity_transformer: Dict[str, Any],
) -> pd.DataFrame:
    """
    Concatenates player clusters with their respective activity record activity data
    and calculates retention ratios based on provided analytical ratios.
    Args:
        user_activity_data: Spark DataFrame containing user retention data.
        players_metadata_clustered: DataFrame containing player data and cluster labels.
        params_ratio_register: Dictionary with ratio register parameters.
        params_activity_transformer: Dictionary with activity transformer parameters.
    Returns:
        Pandas DataFrame containing concatenated player cluster activity data and
        retention ratios
    """
    # Call | Create Spark Session
    spark = SparkSession.builder.getOrCreate()

    # Create Spark DataFrame from Pandas DataFrame
    players_metadata_clustered = spark.createDataFrame(
        data=players_metadata_clustered
    ).select("player_id", "cluster_labels")

    # Join user activity data with player metadata and cluster labels
    player_clustered_activity = user_activity_data.join(
        players_metadata_clustered, on="player_id", how="left"
    )

    # Select retention metric columns and dates of first log records
    ret_columns = player_clustered_activity.select(
        player_clustered_activity.colRegex("`^D\d+R$`")
    ).columns
    player_clustered_activity = player_clustered_activity.select(
        *["player_id", "cluster_labels", "first_log", *ret_columns]
    )
    # Group by cluster labels and first log date, and aggregate retention metrics
    player_clustered_activity = player_clustered_activity.groupBy(
        ["cluster_labels", "first_log"]
    ).agg(*[f.sum(col).alias(f"{col}") for col in ret_columns])

    # Get analytical ratios, normal ratios and days available from parameters,
    # and validate that all day labels are present
    analytical_ratios = [
        literal_eval(ratio) for ratio in params_ratio_register.get("analytical_ratios")
    ]
    ratios = [literal_eval(ratio) for ratio in params_ratio_register["ratios"]]
    days_available = params_activity_transformer.get("retention_days")

    # Convert retention metrics to percentages
    for ratio in ratios:
        num, den = ratio
        ratio_name = f"D{num}R"
        ret_num = f"D{num}R"
        ret_den = f"D{den}R"
        player_clustered_activity = player_clustered_activity.withColumn(
            ratio_name, ratio_agg(f.col(ret_num), f.col(ret_den))
        )

    # Obtain analytical ratios if them were defined in the parameters (ratio register)
    if params_ratio_register["analytical_ratios"] and isinstance(
        params_ratio_register["analytical_ratios"], list
    ):
        # Validate day labels are present in parameters
        _ratio_days_availabilty(analytical_ratios, days_available)

        # Calculate retention ratios for each analytical ratio (inputs)
        for ratio in analytical_ratios:
            num, den = ratio
            ratio_name = f"D{num}/D{den}"
            ret_num = f"D{num}R"
            ret_den = f"D{den}R"
            player_clustered_activity = player_clustered_activity.withColumn(
                ratio_name, ratio_agg(f.col(ret_num), f.col(ret_den))
            )

    # Reorder dataframe, reformat the cluster labels and send to data to driver
    player_clustered_activity = (
        player_clustered_activity.orderBy(f.asc("first_log"), f.asc("cluster_labels"))
        .dropna(subset=["cluster_labels"])
        .withColumn("cluster_labels", f.col("cluster_labels").cast("integer"))
        .toPandas()
    )

    # Present all installations as 100% retention
    player_clustered_activity["D0R"] = float(1.0)

    return player_clustered_activity


def _ratio_days_availabilty(ratios: List[int], days_available: List[int]):
    """Void function to validate days requested in ratios to generate, are present in
    the retention days extracted"""
    for num, den in ratios:
        if (num not in days_available) or (den not in days_available):
            log.warning(
                f'Days requested in "ratios" parameter are not present in the days '
                f'extracted from "Activity Transformer Node". If not working, '
                f'try running "activity_transformer_node" again and check {num} and'
                f' {den} are present in "retention_days" parameters'
            )
            raise
    log.info("Retention days validation passed")


@f.udf(returnType=t.FloatType())
def ratio_agg(ret_num, ret_den):
    """User Defined Function to validate ratio aggregation won't produce a division
    error"""
    if ret_den != 0 and ret_den is not None:
        return float(ret_num) / float(ret_den)
    else:
        return float(0)


def user_retention_plot_gen(
    player_clustered_activity: pd.DataFrame, parameters: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """
    Generate a user retention plot for a given player clustered activity dataframe.
    Args:
        player_clustered_activity: Dataframe containing player clustered activity
        with retention values.
        parameters: A dictionary containing plot format parameters.
    Returns:
        Plotly figure representing the user retention plot
    """
    # Extract the number of days to filter based on the 'last_days_filter' parameter
    days = parameters.get("last_days_filter")

    # Subset the columns that contain User retention values using a regular expression
    retention_columns = [
        col_name
        for col_name in player_clustered_activity.columns
        if re.match(r"D\d+R", col_name)
    ]

    # Group the player clustered activity by date and sum up retention values
    player_clusters_grouped = (
        player_clustered_activity.groupby(["cluster_labels", "first_log"])
        .sum()
        .reset_index()
    )

    # Convert the date strings to datetime objects
    player_clusters_grouped["first_log"] = pd.to_datetime(
        player_clusters_grouped["first_log"]
    )

    # Find the most recent date in the 'first_log' column
    most_recent_date = player_clusters_grouped["first_log"].max()

    # Subset the dataframe to include only the data for the last 'days' number of days
    last_days = most_recent_date - pd.Timedelta(days=days)
    player_clusters_grouped = player_clusters_grouped[
        player_clusters_grouped["first_log"] >= last_days
    ]

    # Create a Plotly figure
    fig = go.Figure()
    traces_labels = []
    # Create a barchart trace for each retention feature
    for cluster_label in player_clusters_grouped["cluster_labels"].unique():
        grouped_by_cluster = player_clusters_grouped[
            player_clusters_grouped["cluster_labels"] == cluster_label
        ]
        for i in retention_columns:
            fig.add_trace(
                go.Bar(
                    x=grouped_by_cluster["first_log"],
                    y=grouped_by_cluster[i],
                    name=f"{i}",
                    visible=False,
                )
            )
            # Append label to the trace
            traces_labels.append(cluster_label)

    # Get the plot dimensions from the 'plot_dimensions' parameter
    plot_dimensions = parameters.get("plot_dimensions")

    # Set the layout for the barchart
    fig.update_layout(
        title=f"User Retention by Date, for last {days} days",
        xaxis_title="Date",
        yaxis_title="Retention",
        barmode="group",
        width=plot_dimensions.get("width"),
        height=plot_dimensions.get("height"),
    )

    # Define default trace to show
    cluster_labels = player_clusters_grouped["cluster_labels"].unique()
    default_label_traces = [
        True if i == cluster_labels[0] else False for i in traces_labels
    ]

    # Define filter options for the dropdown menu
    filter_options = [
        {
            "label": "Select player clusters",
            "method": "update",
            "args": [{"visible": default_label_traces}],
        }
    ]

    # Subset active labels to define button actions
    for cluster in cluster_labels:
        active_traces = [True if i == cluster else False for i in traces_labels]
        option = {
            "label": f"Cluster {cluster}",
            "method": "update",
            "args": [{"visible": active_traces}],
        }
        filter_options.append(option)

    # Add the filter dropdown menu to the layout
    fig.update_layout(
        updatemenus=[
            go.layout.Updatemenu(
                buttons=filter_options,
                direction="left",
                pad={"r": 10, "t": 10},
                showactive=True,
                active=0,
                x=1.0,
                xanchor="left",
                y=1.18,
                yanchor="top",
            )
        ]
    )

    # Set the layout for the barchart
    fig.update_layout(
        title=f"User Retention by Date, for last {days} days",
        xaxis_title="Date",
        yaxis_title="Retention",
        barmode="group",
        width=plot_dimensions.get("width"),
        height=plot_dimensions.get("height"),
    )

    return fig
