"""
This is a boilerplate pipeline 'events_activity_segmentation'
generated using Kedro 0.18.4
"""
import logging
from functools import reduce
from ast import literal_eval
from typing import Dict, List, Tuple, Any
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

log = logging.getLogger(__name__)


def _list_player_tags(event_data: pyspark.sql.DataFrame, players_idxs: List[int]):
    """Helper function to separate and concatenate multiple player tags from the
    'battle_teams' column into a vertical dimension of the input DataFrame (Long
    format) for the given list of player indices. The resulting DataFrame contains
    columns with the player tags for each player, as well as an 'players_exploded'
    column that contain rows the player"""
    # Iterate over player indices and create new columns for each player's tag and
    # corresponding player
    for player_num in players_idxs:
        event_data = (
            event_data.withColumn(
                f"team_player_{player_num}",
                f.col("battle_teams").getItem(player_num - 1),
            )
            .withColumn(
                f"team_player_{player_num}_tag",
                f.col(f"team_player_{player_num}").getItem("tag"),
            )
            .withColumn(
                f"team_player_{player_num}",
                f.col("battle_teams").getItem(player_num - 1),
            )
            .withColumn(
                f"team_player_{player_num}_tag",
                f.col(f"team_player_{player_num}").getItem("tag"),
            )
        )
    # Create a list of column names for the player tags and use it to create a new
    # column 'players_exploded'
    player_tags = [f"team_player_{player_num}_tag" for player_num in players_idxs]
    event_data = event_data.withColumn("players_exploded", f.array(*player_tags))
    return event_data


def _list_player_brawlers(
    MapStructure: t.Any, event_data: pyspark.sql.DataFrame, players_idxs: List[int]
):
    """Helper function to convert multiple player brawler names into a vertical
    dimension of the DataFrame (Long format)"""
    # Iterate over player indices and create new columns for each player's brawler and
    # corresponding player
    for player_num in players_idxs:
        event_data = (
            event_data.withColumn(
                f"team_player_{player_num}_brawler",
                f.col(f"team_player_{player_num}").getItem("brawler"),
            )
            .withColumn(
                f"team_player_{player_num}_brawler",
                f.from_json(f"team_player_{player_num}_brawler", MapStructure),
            )
            .withColumn(
                f"team_player_{player_num}_brawler_name",
                f.col(f"team_player_{player_num}_brawler").getItem("name"),
            )
        )
    # Create a list of column names for the player brawler and use it to create a new
    # column 'brawlers_exploded'
    brawler_names = [
        f"team_player_{player_num}_brawler_name" for player_num in players_idxs
    ]
    event_data = event_data.withColumn("brawlers_exploded", f.array(*brawler_names))
    return event_data


def _group_exploder_solo(
    event_solo_data: pyspark.sql.DataFrame, standard_columns: List[str]
) -> pyspark.sql.DataFrame:
    """Helper function that extracts solo-events information from
    dictionary-formatted columns"""
    try:
        # Explode the list of player dictionaries as string types
        event_solo_data = event_solo_data.withColumn(
            "battle_players", f.explode("battle_players")
        )

        # Convert the dictionary of players from StringTypes to MapTypes
        MapStructure = t.MapType(t.StringType(), t.StringType())
        event_solo_data = event_solo_data.withColumn(
            "battle_players", f.from_json("battle_players", MapStructure)
        )

        # Extract player tags and brawlers
        event_solo_data = event_solo_data.withColumn(
            "tag", f.col("battle_players").getItem("tag")
        ).withColumn("brawler", f.col("battle_players").getItem("brawler"))

        # Convert the dictionary with brawlers as StringTypes to MapTypes and extract
        # the brawler attribute
        event_solo_data = event_solo_data.withColumn(
            "brawler", f.from_json("brawler", MapStructure)
        )
        event_solo_data = event_solo_data.withColumn(
            "brawler", f.col("brawler").getItem("name")
        )

        # Convert the dataframe to a flattened structure by grouping on standard
        # columns and aggregating player and brawler collections
        event_solo_data = event_solo_data.groupBy(standard_columns).agg(
            f.collect_list("tag").alias("players_collection"),
            f.collect_list("brawler").alias("brawlers_collection"),
        )
    except Exception as e:
        log.exception(e)
        log.warning(
            "Failed to extract solo-events information from the 'battle_players' "
            "column. Check if the column has a consistent structure according to API "
            "specs."
        )
        standard_columns.extend(["battle_players"])
        event_solo_data = (
            event_solo_data.select(*standard_columns)
            .withColumn(
                "players_collection",
                f.lit("['null']").cast(t.ArrayType(t.StringType())),
            )
            .withColumn(
                "brawlers_collection",
                f.lit("['null']").cast(t.ArrayType(t.StringType())),
            )
        )
    return event_solo_data


def _group_exploder_duo(
    event_duo_data: pyspark.sql.DataFrame, standard_columns: List[str]
) -> pyspark.sql.DataFrame:
    """Helper function to subset duo-events information from dictionary-formatted
    columns"""
    try:
        # Explode the list of player dictionaries as string types
        event_duo_data = event_duo_data.withColumn(
            "battle_teams", f.explode("battle_teams")
        )

        # Convert the dictionary of players from string types to array of map types
        MapStructure = t.MapType(t.StringType(), t.StringType())
        event_duo_data = event_duo_data.withColumn(
            "battle_teams", f.from_json("battle_teams", t.ArrayType(MapStructure))
        )

        # Extract and concatenate the tags of two players
        event_duo_data = _list_player_tags(
            event_data=event_duo_data, players_idxs=[1, 2]
        )

        # Extract and concatenate the brawlers of two players
        event_duo_data = _list_player_brawlers(
            MapStructure=MapStructure, event_data=event_duo_data, players_idxs=[1, 2]
        )

        # Group the exploded groups into a Spark list object
        event_duo_data = event_duo_data.groupBy(standard_columns).agg(
            f.collect_list("players_exploded").alias("players_collection"),
            f.collect_list("brawlers_exploded").alias("brawlers_collection"),
        )
    except Exception as e:
        log.exception(e)
        log.warning(
            "-- Exploder broken: Check 'battle_teams' column has a consistent "
            "structure according API specs --"
        )
        # If there's an error, create null columns for the players and brawlers
        # collections
        standard_columns.extend(["battle_teams"])
        event_duo_data = (
            event_duo_data.select(*standard_columns)
            .withColumn(
                "players_collection",
                f.lit("['null']").cast(t.ArrayType(t.StringType())),
            )
            .withColumn(
                "brawlers_collection",
                f.lit("['null']").cast(t.ArrayType(t.StringType())),
            )
        )
    return event_duo_data


def _group_exploder_3v3(
    event_3v3_data: pyspark.sql.DataFrame, standard_columns: List[str]
) -> pyspark.sql.DataFrame:
    """Helper function to subset 3v3 events information from dictionary-formatted
    columns"""
    try:
        # Explode the list of player dictionaries as string types
        event_3v3_data = event_3v3_data.withColumn(
            "battle_teams", f.explode("battle_teams")
        )

        # Convert the dictionary of players from string types to array of map types
        MapStructure = t.MapType(t.StringType(), t.StringType())
        event_3v3_data = event_3v3_data.withColumn(
            "battle_teams", f.from_json("battle_teams", t.ArrayType(MapStructure))
        )

        # Extract and concatenate the tags of three players
        event_3v3_data = _list_player_tags(
            event_data=event_3v3_data, players_idxs=[1, 2, 3]
        )

        # Extract and concatenate the brawlers of three players
        event_3v3_data = _list_player_brawlers(
            MapStructure=MapStructure, event_data=event_3v3_data, players_idxs=[1, 2, 3]
        )

        # Group the exploded groups into a Spark list object
        event_3v3_data = event_3v3_data.groupBy(standard_columns).agg(
            f.collect_list("players_exploded").alias("players_collection"),
            f.collect_list("brawlers_exploded").alias("brawlers_collection"),
        )

    except Exception as e:
        log.exception(e)
        log.warning(
            "-- Exploder broken: Check 'battle_teams' column has a consistent "
            "structure according API specs --"
        )
        # If there's an error, create null columns for the players and brawlers
        # collections
        standard_columns.extend(["battle_teams"])
        event_3v3_data = (
            event_3v3_data.select(*standard_columns)
            .withColumn(
                "players_collection",
                f.lit("['null']").cast(t.ArrayType(t.StringType())),
            )
            .withColumn(
                "brawlers_collection",
                f.lit("['null']").cast(t.ArrayType(t.StringType())),
            )
        )
    return event_3v3_data


def _group_exploder_special(
    event_special_data: pyspark.sql.DataFrame, standard_columns: List[str]
) -> pyspark.sql.DataFrame:
    """Helper function to subset special events information from dictionary-formatted
    columns"""
    try:
        # Explode the list of player dictionaries as string types
        event_special_data = event_special_data.withColumn(
            "battle_players", f.explode("battle_players")
        )

        # Convert the dictionary of players from string types to array of map types
        MapStructure = t.MapType(t.StringType(), t.StringType())
        event_special_data = event_special_data.withColumn(
            "battle_players", f.from_json("battle_players", MapStructure)
        )

        # Separate and concatenate player's tags and brawlers
        event_special_data = (
            event_special_data.withColumn(
                "team_player_tag", f.col("battle_players").getItem("tag")
            )
            .withColumn(
                "team_player_brawler", f.col("battle_players").getItem("brawler")
            )
            .withColumn(
                "team_player_brawler", f.from_json("team_player_brawler", MapStructure)
            )
            .withColumn(
                "team_player_brawler_name", f.col("team_player_brawler").getItem("name")
            )
        )
        standard_columns.extend(
            ["battle_bigBrawler_tag", "battle_bigBrawler_brawler_name"]
        )
        # Group the exploded groups into a Spark list object
        event_special_data = event_special_data.groupBy(standard_columns).agg(
            f.collect_list("team_player_tag").alias("players_collection"),
            f.collect_list("team_player_brawler_name").alias("brawlers_collection"),
        )
    except Exception as e:
        log.exception(e)
        log.warning(
            "-- Exploder broken: Check 'battle_players' column has a consistent "
            "structure according API specs --"
        )
        # If there's an error, create null columns for the players and brawlers
        # collections
        standard_columns.extend(["battle_players"])
        event_special_data = (
            event_special_data.select(*standard_columns)
            .withColumn(
                "players_collection",
                f.lit("['null']").cast(t.ArrayType(t.StringType())),
            )
            .withColumn(
                "brawlers_collection",
                f.lit("['null']").cast(t.ArrayType(t.StringType())),
            )
        )
    return event_special_data


def battlelogs_deconstructor(
    battlelogs_filtered: pyspark.sql.DataFrame, parameters: Dict[str, Any]
) -> Tuple[pyspark.sql.DataFrame]:
    """
    Extract player and brawler combinations from the same team or opponents,
    grouped in JSON format, based on user-defined event types.
    Each of the 'exploders' is parameterized by the user, according to the number of
    players needed for each type of event.
    Args:
        battlelogs_filtered: a filtered Pyspark DataFrame containing relevant cohorts
        and features.
        parameters: a dictionary of event types defined by the user to include in the
        subset process.
    Returns:
        A tuple of four Pyspark DataFrames, each containing only one event type
    """
    # Call | Create Spark Session
    spark = SparkSession.builder.getOrCreate()

    log.info("Deconstructing Solo Events")
    if parameters["event_solo"] and isinstance(parameters["event_solo"], list):
        event_solo_data = battlelogs_filtered.filter(
            f.col("event_mode").isin(parameters["event_solo"])
        )
        event_solo_data = _group_exploder_solo(
            event_solo_data, parameters["standard_columns"]
        )
    else:
        log.warning(
            "Solo Event modes not defined or not found according to parameter list"
        )
        event_solo_data = spark.createDataFrame([], schema=t.StructType([]))

    log.info("Deconstructing Duo Events")
    if parameters["event_duo"] and isinstance(parameters["event_duo"], list):
        event_duo_data = battlelogs_filtered.filter(
            f.col("event_mode").isin(parameters["event_duo"])
        )
        event_duo_data = _group_exploder_duo(
            event_duo_data, parameters["standard_columns"]
        )
    else:
        log.warning(
            "Duo Event modes not defined or not found according to parameter list"
        )
        event_duo_data = spark.createDataFrame([], schema=t.StructType([]))

    log.info("Deconstructing 3 vs 3 Events")
    if parameters["event_3v3"] and isinstance(parameters["event_3v3"], list):
        event_3v3_data = battlelogs_filtered.filter(
            f.col("event_mode").isin(parameters["event_3v3"])
        )
        event_3v3_data = _group_exploder_3v3(
            event_3v3_data, parameters["standard_columns"]
        )
    else:
        log.warning(
            "3 vs 3 Event modes not defined or not found according to parameter list"
        )
        event_3v3_data = spark.createDataFrame([], schema=t.StructType([]))

    log.info("Deconstructing Special Events")
    if parameters["event_special"] and isinstance(parameters["event_special"], list):
        event_special_data = battlelogs_filtered.filter(
            f.col("event_mode").isin(parameters["event_special"])
        )
        event_special_data = _group_exploder_special(
            event_special_data, parameters["standard_columns"]
        )
    else:
        log.warning(
            "Special Event modes not defined or not found according to parameter list"
        )
        event_special_data = spark.createDataFrame([], schema=t.StructType([]))

    return event_solo_data, event_duo_data, event_3v3_data, event_special_data


@f.udf(returnType=t.IntegerType())
def retention_metric(days_activity_list, day):
    """User Defined Function to return user retention, based on the given definition"""
    # Extract the maximum day retained
    for i in reversed(range(len(days_activity_list))):
        if days_activity_list[i] == 1:
            max_day_ret = i
            break
    # Subset columns of days needed for evaluation
    days_activity = days_activity_list[: day + 1]
    # If player never installed the game, return 0
    if days_activity[0] != 1:
        return 0
    # If user returned after more days than the one evaluated, return 1
    elif day <= max_day_ret:
        return 1
    else:
        # Evaluate the day of retention is on the data, otherwise return 0
        if day > len(days_activity):
            return 0
        else:
            # Evaluate user installed the app and returned on the specific day, return 1
            if days_activity[0] == 1 and days_activity[-1] == 1:
                return 1
            else:
                return 0


@f.udf(returnType=t.IntegerType())
def sessions_sum(daily_sessions_list, day):
    """Return the total number of sessions accumulated within a given range"""
    # Slice the list to only include the specified range of days
    daily_sessions = daily_sessions_list[: day + 1]
    # Calculate the total number of sessions within the specified range
    total = 0
    for i in range(len(daily_sessions)):
        if daily_sessions[i] is not None:
            total += int(daily_sessions[i])
    return total


def activity_transformer(
    battlelogs_filtered: pyspark.sql.DataFrame, parameters: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """
    Converts filtered battlelogs activity into a wrapped format data frame,
    with retention metrics and n-sessions at the player level of granularity. It
    takes a set of parameters to extract the retention and number of sessions based
    on a 'cohort frequency' parameter.
    Args:
        battlelogs_filtered: Filtered Pyspark DataFrame containing only cohorts and
        features required for the study.
        parameters: Frequency of the cohort and days to extract for the output.
    Returns:
        Pyspark dataframe with retention metrics and n-sessions at the player level
        of granularity.
    Notes (parameters):
    - When 'daily' granularity is selected, the cohort period is the day of the
    user's  first event.
    - When 'weekly' granularity is selected, the cohort period is the first
    consecutive Monday on or after the day of the user's first event.
    - When 'monthly' granularity is selected, the cohort period is the first day of
    the month in which the user's first event occurred.
    Notes (performance):
    - The Cohort frequency transformation skips exhaustive intermediate transformations,
    such as 'Sort', since the user can insert many cohorts as they occur in the
    preprocessing stage, causing excessive partitioning.
    """
    # Aggregate user activity data to get daily number of sessions
    user_activity = (
        battlelogs_filtered.select("cohort", "battleTime", "player_id")
        .groupBy("cohort", "battleTime", "player_id")
        .count()
        .withColumnRenamed("count", "daily_sessions")
    )

    # Validate | Redefine Cohort Frequency, default to 'daily'
    time_freq, user_activity = _convert_user_activity_frequency(
        cohort_frequency=parameters["cohort_frequency"], user_activity=user_activity
    )

    # Create a window by each one of the player's window
    player_window = Window.partitionBy(["player_id"])

    # Find the first logged date per player in the cohort
    user_activity = user_activity.withColumn(
        "first_log", f.min(time_freq).over(player_window)
    )

    # Find days passed to see player return
    user_activity = user_activity.withColumn(
        "days_to_return", f.datediff(time_freq, "first_log")
    )

    # Subset columns and add counter variable to aggregate number of player further
    user_activity = user_activity.select(
        "cohort", "player_id", "first_log", "days_to_return", "daily_sessions"
    ).withColumn("player_count", f.lit(1))

    # List cohorts, iterate over them and append them to the final output
    cohort_list = [
        row.cohort for row in user_activity.select("cohort").distinct().collect()
    ]
    output_range = []

    for cohort in cohort_list:
        # Filter only data of the cohort in process
        tmp_user_activity = user_activity.filter(f.col("cohort") == cohort)

        # Pivot data from long format to wide
        tmp_user_activity = (
            tmp_user_activity.groupBy(["player_id", "first_log"])
            .pivot("days_to_return")
            .agg(
                f.sum("player_count").alias("day"),
                f.sum("daily_sessions").alias("day_sessions"),
            )
        )

        # Extract daily activity columns, save as arrays of column objects
        day_cohort_col = tmp_user_activity.select(
            tmp_user_activity.colRegex("`.+_day$`")
        ).columns
        day_cohort_col = f.array(*map(f.col, day_cohort_col))

        # Extract daily sessions counter columns, save as arrays of column objects
        day_sessions_col = tmp_user_activity.select(
            tmp_user_activity.colRegex("`.+_day_sessions$`")
        ).columns
        day_sessions_col = f.array(*map(f.col, day_sessions_col))

        # Empty list to allocate final columns
        cols_retention = []

        # Produce retention metrics and session counters
        for day in parameters["retention_days"]:
            # Define column names
            DDR = f"D{day}R"
            DDTS = f"D{day}_Sessions"
            # Obtain retention for a given day
            tmp_user_activity = tmp_user_activity.withColumn(
                DDR, retention_metric(day_cohort_col, f.lit(day))
            )
            # Obtain total of sessions until given day
            tmp_user_activity = tmp_user_activity.withColumn(
                DDTS, sessions_sum(day_sessions_col, f.lit(day))
            ).withColumn(DDTS, f.when(f.col(DDR) != 1, 0).otherwise(f.col(DDTS)))
            # Append final columns
            cols_retention.append(DDR)
            cols_retention.append(DDTS)

        # Final formatting
        standard_columns = ["player_id", "first_log"]
        standard_columns.extend(cols_retention)
        tmp_user_activity = tmp_user_activity.select(*standard_columns)

        # Append cohort's user activity data to final list
        output_range.append(tmp_user_activity)

    # Reduce all dataframe to overwrite original
    user_activity_data = reduce(DataFrame.unionAll, output_range)

    return user_activity_data


def _convert_user_activity_frequency(cohort_frequency, user_activity):
    """Helper function to convert the actual user activity frequency to level
    defined by the parameter 'cohort_frequency'"""
    if cohort_frequency and isinstance(cohort_frequency, str):
        # Construct the cohort on a weekly basis
        if cohort_frequency == "weekly":
            time_freq = "weekly_battleTime"
            user_activity = user_activity.withColumn(
                time_freq, f.date_sub(f.next_day("battleTime", "Monday"), 7)
            )
        # Construct the cohort on a monthly basis
        elif cohort_frequency == "monthly":
            time_freq = "monthly_battleTime"
            user_activity = user_activity.withColumn(
                time_freq, f.trunc("battleTime", "month")
            )
        # Construct the cohort on a daily basis (default)
        else:
            time_freq = "battleTime"
    else:
        time_freq = "battleTime"
    return time_freq, user_activity


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


def ratio_register(
    user_activity: pyspark.sql.DataFrame,
    params_rat_reg: Dict[str, Any],
    params_act_tran: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    """
    Takes Activity per Day from the "activity_transformer_node" (E.G: columns DXR),
    builds retention ratios per day using a bounded retention calculation.
    Args:
        user_activity: Pyspark dataframe with retention metrics and n-sessions at the
        player level of granularity.
        params_rat_reg: Parameters to aggregate retention metrics, based on User inputs.
        params_act_tran: Parameters to extract activity day per user, based on User.
        inputs.
    Returns:
        Pyspark dataframe with bounded retention ratios aggregated.
    """
    # Request parameters to validate
    days_available = params_act_tran["retention_days"]
    ratios = [literal_eval(ratio) for ratio in params_rat_reg["ratios"]]

    # Validate day labels are present in parameters
    _ratio_days_availabilty(ratios, days_available)

    # Grouping + renaming multiple columns: https://stackoverflow.com/a/74881697
    retention_columns = [
        col for col in user_activity.columns if col not in ["player_id", "first_log"]
    ]
    aggs = [f.expr(f"sum({col}) as {col}") for col in retention_columns]
    # Apply aggregation
    cohort_activity_data = user_activity.groupBy("first_log").agg(*aggs)

    # Obtain basics retention ratios aggregated
    for ratio in ratios:
        num, den = ratio
        ratio_name = f"D{num}R"
        ret_num = f"D{num}R"
        ret_den = f"D{den}R"
        cohort_activity_data = cohort_activity_data.withColumn(
            ratio_name, ratio_agg(f.col(ret_num), f.col(ret_den))
        )

    # Obtain analytical ratios if them were defined in the parameters (ratio register)
    if params_rat_reg["analytical_ratios"] and isinstance(
        params_rat_reg["analytical_ratios"], list
    ):
        # Request parameters to validate
        analytical_ratios = [
            literal_eval(ratio) for ratio in params_rat_reg["analytical_ratios"]
        ]
        # Validate day labels are present in parameters
        _ratio_days_availabilty(analytical_ratios, days_available)
        # Obtain basics retention ratios aggregated
        for ratio in analytical_ratios:
            num, den = ratio
            ratio_name = f"D{num}/D{den}"
            ret_num = f"D{num}R"
            ret_den = f"D{den}R"
            cohort_activity_data = cohort_activity_data.withColumn(
                ratio_name, ratio_agg(f.col(ret_num), f.col(ret_den))
            )
    else:
        log.info("No analytical ratios were defined")

    # Reorganization per log date
    cohort_activity_data = cohort_activity_data.orderBy(["first_log"])

    return cohort_activity_data
