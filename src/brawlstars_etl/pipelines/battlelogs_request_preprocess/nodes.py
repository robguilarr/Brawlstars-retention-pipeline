"""
This is a boilerplate pipeline 'battlelogs_request_preprocess'
generated using Kedro 0.18.4
Note: To load the Kedro configuration:
https://kedro.readthedocs.io/en/stable/kedro_project_setup/configuration.html#credentials
"""
import pandas as pd
import numpy as np
import brawlstats
import datetime as dt
import time
import logging
import asyncio
from typing import Dict, Any
from ast import literal_eval
from functools import reduce
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t

from kedro.config import ConfigLoader
from kedro.framework.project import settings

conf_loader_local = ConfigLoader(conf_source=settings.CONF_SOURCE, env="local")
conf_credentials = conf_loader_local["credentials"]

log = logging.getLogger(__name__)


def battlelogs_request(
    player_tags_txt: str, parameters: Dict[str, Any]
) -> pd.DataFrame:
    """
    Extracts battlelogs from Brawlstars API by asynchronously executing a list of
    futures objects, each of which is made up of an Async thread task due to blocking
    call limitations of the api_request submodule.
    Args:
        player_tags_txt: A list of player tags.
        parameters: Additional parameters to be included in the API request.
    Returns:
         A structured DataFrame that concatenates all the battlelogs of the players.
    """
    # Get key and validate it exists
    API_KEY = conf_credentials.get("brawlstars_api", None).get("API_KEY", None)
    try:
        assert API_KEY is not None
    except AssertionError:
        log.info(
            "No API key has been defined. Request one at https://developer.brawlstars.com/"
        )

    # Create a BrawlStats client object to interact with the API. Note: the
    # "prevent_ratelimit" function in the source code can be used this behavior
    client = brawlstats.Client(token=API_KEY)

    # Split the player tags text by commas to create a list of player tags.
    player_tags_txt = player_tags_txt.split(",")

    def api_request(tag: str) -> pd.DataFrame:
        """Requests and structures the 25 most recent battle logs from the Brawl
        Stars API for a given player."""
        try:
            # Retrieve raw battle logs data from API
            player_battle_logs = client.get_battle_logs(tag).raw_data
            # Normalize data into structured format
            player_battle_logs_structured = pd.json_normalize(player_battle_logs)
            # Add player ID to DataFrame
            player_battle_logs_structured["player_id"] = tag
        except:
            log.info(f"No battle logs extracted for player {tag}")
            player_battle_logs_structured = pd.DataFrame()
            pass
        return player_battle_logs_structured

    async def api_request_async(tag: str) -> pd.DataFrame:
        """
        Transform non-sync request function to async coroutine, which creates
        a future object by API request.
        The Coroutine contains a blocking call that won't return a log until it's
        complete. So, to run concurrently, await the thread and not the coroutine by
        using this method.
        """
        return await asyncio.to_thread(api_request, tag)

    async def spawn_request(player_tags_txt: list) -> pd.DataFrame:
        """
        This asynchronous function takes a list of player tags as input and creates a
        list of coroutines that request the player's battlelogs data. It then
        schedules their execution and waits for them to complete by using the
        asyncio.gather() method. Finally, it concatenates all the dataframes returned by
        the coroutines into one dataframe.
        """
        start = time.time()
        log.info(f"Battlelogs request process started")
        # Create a list of coroutines and schedule their execution
        requests_tasks = [
            asyncio.create_task(api_request_async(tag)) for tag in player_tags_txt
        ]
        # Wait for all tasks to complete and gather their results
        battlelogs_data_list = await asyncio.gather(*requests_tasks)
        # Concatenate all the dataframes returned by the coroutines into one
        raw_battlelogs = pd.concat(battlelogs_data_list, ignore_index=True)
        log.info(
            f"Battlelogs request process finished in {time.time() - start} seconds"
        )
        return raw_battlelogs

    def activate_request(n: int = None) -> pd.DataFrame:
        """
        This function checks if a sampling limit is provided, and if so, it calls the
        spawn_request() function to request the battlelogs data of the first n
        players. If n is not provided, it splits the player tags list into batches of 10
        and calls the spawn_request() function for each batch. It then concatenates
        the dataframes returned by each call into one.
        """
        raw_battlelogs = pd.DataFrame()
        if n:
            # Call spawn_request() for the first n players
            raw_battlelogs = asyncio.run(spawn_request(player_tags_txt[:n]))
        else:
            # Split the tags list into batches of 10 and call spawn_request() for each
            split_tags = np.array_split(player_tags_txt, len(player_tags_txt) / 10)
            for batch in split_tags:
                raw_battlelogs_tmp = asyncio.run(spawn_request(batch))
                # Concatenate the dataframes returned by each call into one
                try:
                    raw_battlelogs = pd.concat(
                        [raw_battlelogs, raw_battlelogs_tmp], axis=0, ignore_index=True
                    )
                except:
                    pass

        return raw_battlelogs

    # Call activate_request() with the specified battlelogs_limit parameter
    raw_battlelogs = activate_request(n=parameters["battlelogs_limit"])

    # Replace dots in column names with underscores
    raw_battlelogs.columns = [
        col_name.replace(".", "_") for col_name in raw_battlelogs.columns
    ]

    # Check if the data request was successful
    try:
        assert not raw_battlelogs.empty
    except AssertionError:
        # Log an error message if no Battlelogs were extracted
        log.info("No Battlelogs were extracted. Please check your Client Connection")

    return raw_battlelogs


def battlelogs_filter(
    raw_battlelogs: pd.DataFrame, parameters: Dict
) -> pyspark.sql.DataFrame:
    """
    Filter players into cohorts (samples) for a predefined study using time ranges
    specified in the parameters. Also transforms datetime format from ISO 8601 to
    java.util.GregorianCalendar for Spark processing.

    Args:
        raw_battlelogs: Dataframe of concatenated battlelogs for all players
        parameters: Dictionary containing time ranges and DDL schema

    Returns:
        Filtered PySpark DataFrame containing only cohorts and necessary features
    """
    # Call | Create Spark Session
    spark = SparkSession.builder.getOrCreate()

    # Load and validate battlelogs data against the provided DDL schema
    try:
        battlelogs_filtered = spark.createDataFrame(
            data=raw_battlelogs, schema=parameters["raw_battlelogs_schema"][0]
        )
        # Convert lists stored as strings to Arrays of dictionaries
        battlelogs_filtered = battlelogs_filtered.withColumn(
            "battle_teams", f.from_json("battle_teams", t.ArrayType(t.StringType()))
        ).withColumn(
            "battle_players", f.from_json("battle_players", t.ArrayType(t.StringType()))
        )
    except TypeError:
        # If DDL schema for the battlelogs is incorrect, raise an error
        log.warning(
            "Type error on the DDL schema for the battlelogs,"
            'check "raw_battlelogs_schema" on the node parameters'
        )
        raise

    # Exclude missing events ids (519325->483860, not useful for activity aggregations)
    if parameters["exclude_missing_events"]:
        battlelogs_filtered = battlelogs_filtered.filter("event_id != 0")

    # Validate if a player is a battlestar player
    battlelogs_filtered = battlelogs_filtered.withColumn(
        "is_starPlayer",
        f.when(f.col("battle_starPlayer_tag") == f.col("player_id"), 1).otherwise(0),
    )

    # Filter the battlelogs data based on the specified timestamp range, and extract
    # a separate cohort to exclude specific dates if required
    if parameters["cohort_time_range"]:
        if "battleTime" not in battlelogs_filtered.columns:
            raise ValueError('Dataframe does not contain "battleTime" column.')
        else:
            # Convert timestamp strings to date format, where original timestamps are
            # in ~ ISO 8601 format (e.g. 20230204T161026.000Z)
            convertDT = f.udf(
                lambda string: dt.datetime.strptime(string, "%Y%m%dT%H%M%S.%fZ").date()
            )
            battlelogs_filtered = battlelogs_filtered.withColumn(
                "battleTime", convertDT("battleTime")
            )
            # Convert Java.util.GregorianCalendar date format to PySpark
            pattern = (
                r"(?:.*)YEAR=(\d+).+?MONTH=(\d+).+?DAY_OF_MONTH=(\d+)"
                r".+?HOUR=(\d+).+?MINUTE=(\d+).+?SECOND=(\d+).+"
            )
            battlelogs_filtered = battlelogs_filtered.withColumn(
                "battleTime",
                f.regexp_replace("battleTime", pattern, "$1-$2-$3 $4:$5:$6").cast(
                    "timestamp"
                ),
            ).withColumn("battleTime", f.date_format("battleTime", format="yyyy-MM-dd"))

        # Initialize an empty list to store DataFrames for each cohort
        cohort_selection = []
        # Assign an integer value to identify each sample cohort
        cohort_num = 1
        # Iterate over the cohort_time_range to subset the battlelogs_filtered DataFrame
        for date_range in parameters["cohort_time_range"]:
            # Filter the DataFrame based on the specified time range
            cohort_range = battlelogs_filtered.filter(
                (f.col("battleTime") > literal_eval(date_range)[0])
                & (f.col("battleTime") < literal_eval(date_range)[1])
            )
            # Add a new column to the DataFrame to identify the cohort it belongs to
            cohort_range = cohort_range.withColumn("cohort", f.lit(cohort_num))
            # Increment the cohort number for the next iteration
            cohort_num += 1
            # Append the cohort DataFrame to the cohort_selection list
            cohort_selection.append(cohort_range)
        # Concatenate all cohort DataFrames into one
        battlelogs_filtered = reduce(DataFrame.unionAll, cohort_selection)

    else:
        log.warning(
            "Please define at least one time range for your cohort. Check the "
            "parameters."
        )
        raise

    # Add a unique identifier column to the DataFrame
    battlelogs_filtered = battlelogs_filtered.withColumn(
        "battlelog_id", f.monotonically_increasing_id()
    )

    return battlelogs_filtered
