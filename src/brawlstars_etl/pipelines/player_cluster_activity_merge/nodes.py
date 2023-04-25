"""
This is a boilerplate pipeline 'player_cluster_activity_merge'
generated using Kedro 0.18.4
"""

import logging
from ast import literal_eval
from typing import Dict, List, Tuple, Any
import pandas as pd
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

    # Get analytical ratios and days available from parameters, and validate that all
    # day labels are present
    analytical_ratios = [
        literal_eval(ratio) for ratio in params_ratio_register.get("analytical_ratios")
    ]
    days_available = params_activity_transformer.get("retention_days")

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

    # print(f"Player Metadata has {8162} unique player tags")
    # print(f"User Activity Data has {9464} unique player tags")

    # Reorder dataframe, reformat the cluster labels and send to data to driver
    player_clustered_activity = (
        player_clustered_activity.orderBy(f.asc("first_log"), f.asc("cluster_labels"))
        .dropna(subset=["cluster_labels"])
        .withColumn("cluster_labels", f.col("cluster_labels").cast("integer"))
        .toPandas()
    )

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
