"""
This is a boilerplate pipeline 'battlelogs_request_preprocess'
generated using Kedro 0.18.4
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

# To load the configuration (https://kedro.readthedocs.io/en/stable/kedro_project_setup/configuration.html#credentials)
from kedro.config import ConfigLoader
from kedro.framework.project import settings

conf_loader_local = ConfigLoader(conf_source=settings.CONF_SOURCE, env="local")
conf_credentials = conf_loader_local["credentials"]

log = logging.getLogger(__name__)


def battlelogs_request(
    player_tags_txt: str, parameters: Dict[str, Any]
) -> pd.DataFrame:
    """
    Extracts Battlelogs from Brawlstars API by executing an Async Event Loop over a
    list of futures objects. These are made of task objects built of Async threads
    due blocking call limitations of api_request sub_module.
    Args:
        player_tags_txt: Player tag list
        parameters: Additional parameters to be passed in the API request
    Returns:
        All players battlelogs concatenated into a structured Dataframe
    """
    # Get key and validate it exists
    API_KEY = conf_credentials.get("brawlstars_api", None).get("API_KEY", None)
    try:
        assert API_KEY != None
    except AssertionError:
        log.info(
            "No API key has been defined. Request one at https://developer.brawlstars.com/"
        )

    # Create client object from brawlstats API wrapper, be aware of preventing the
    # rate limit for huge requests, review prevent_ratelimit in the source code
    client = brawlstats.Client(token=API_KEY)

    # Create list of player tags, from catalog
    player_tags_txt = player_tags_txt.split(",")

    def api_request(tag: str) -> pd.DataFrame:
        """Request battlelogs from the Brawl Stars API and give a structured format"""
        try:
            # Extract list of 25 most recent session logs
            player_battle_logs = client.get_battle_logs(tag).raw_data
            # Normalize data in structured format
            player_battle_logs_structured = pd.json_normalize(player_battle_logs)
            player_battle_logs_structured["player_id"] = tag
        except:
            log.info(f"No Battlelog extracted for player {tag}")
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
        """Use gathering to request battlelogs as async tasks objects, made of
        coroutines"""
        start = time.time()
        log.info(f"Battlelogs request process started")
        # Comprehensive list of coroutines as Task Objects, whom will be already
        # scheduled its execution
        requests_tasks = [
            asyncio.create_task(api_request_async(tag)) for tag in player_tags_txt
        ]
        # Future Object: List of battlelogs as Dataframes
        battlelogs_data_list = await asyncio.gather(*requests_tasks)
        # When all tasks all executed, concat all dataframes into one
        raw_battlelogs = pd.concat(battlelogs_data_list, ignore_index=True)
        log.info(
            f"Battlelogs request process Finished in {time.time() - start} seconds"
        )
        return raw_battlelogs

    def activate_request(n: int = None) -> pd.DataFrame:
        """Run the events-loop, check for request limit defined by user"""
        raw_battlelogs = pd.DataFrame()
        if n:
            # For sampling purposes
            raw_battlelogs = asyncio.run(spawn_request(player_tags_txt[:n]))
        else:
            # For running entire batches (prevent being rate-limited)
            split_tags = np.array_split(player_tags_txt, len(player_tags_txt) / 10)
            for batch in split_tags:
                raw_battlelogs_tmp = asyncio.run(spawn_request(batch))
                try:
                    raw_battlelogs = pd.concat(
                        [raw_battlelogs, raw_battlelogs_tmp], axis=0, ignore_index=True
                    )
                except:
                    pass

        return raw_battlelogs

    raw_battlelogs = activate_request(n=parameters["battlelogs_limit"])

    # Replace dots in column names
    raw_battlelogs.columns = [
        col_name.replace(".", "_") for col_name in raw_battlelogs.columns
    ]

    # Validate concurrency didn't affect the data request
    try:
        assert not raw_battlelogs.empty
    except AssertionError:
        log.info("No Battlelogs were extracted. Please check your Client Connection")

    return raw_battlelogs


def battlelogs_filter(
    raw_battlelogs: pd.DataFrame, parameters: Dict
) -> pyspark.sql.DataFrame:
    """
    Filter a variety of players into cohorts, also known as samples for a pre-defined
    study. To take advantage of its use, verify that at least one time range is
    defined within the battlelogs_request_preprocess pipeline parameters.
    This node also applies a user-defined function that transforms the raw 'datetime'
    (ISO 8601) from the Brawl Stars API, then transforms it to
    java.util.GregorianCalendar (low-level date format) for Spark processing.
    Args:
        raw_battlelogs: All players battlelogs concatenated into a structured Dataframe
        parameters: Time range(s) to subset and DDL schema
    Returns:
        Filtered Pyspark DataFrame containing only cohorts and features required for
        the study
    """
    # Call | Create Spark Session
    spark = SparkSession.builder.getOrCreate()

    # Ingest battlelogs data and validate against DDL schema
    try:
        battlelogs_filtered = spark.createDataFrame(
            data=raw_battlelogs, schema=parameters["raw_battlelogs_schema"][0]
        )
        # Convert strings of lists as Arrays of dictionaries
        battlelogs_filtered = battlelogs_filtered.withColumn(
            "battle_teams", f.from_json("battle_teams", t.ArrayType(t.StringType()))
        ).withColumn(
            "battle_players", f.from_json("battle_players", t.ArrayType(t.StringType()))
        )
    except TypeError:
        log.warning(
            "Type error on the DDL schema for the battlelogs,"
            'check "raw_battlelogs_schema" on the node parameters'
        )
        raise

    # Exclude missing events ids (519325->483860, not useful for activity aggregations)
    if parameters["exclude_missing_events"]:
        battlelogs_filtered = battlelogs_filtered.filter("event_id != 0")

    # Battlestar player validation
    battlelogs_filtered = battlelogs_filtered.withColumn(
        "is_starPlayer",
        f.when(f.col("battle_starPlayer_tag") == f.col("player_id"), 1).otherwise(0),
    )

    # Filter date based on timestamp, separated cohort can be extracted to exclude
    # specific dates
    if parameters["cohort_time_range"]:
        if "battleTime" not in battlelogs_filtered.columns:
            raise ValueError('Check dataframe contains "battleTime" column')
        else:
            # Convert string to date format, original timestamps are in ~ ISO 8601
            # format ex: 20230204T161026.000Z
            convertDT = f.udf(
                lambda string: dt.datetime.strptime(string, "%Y%m%dT%H%M%S.%fZ").date()
            )
            battlelogs_filtered = battlelogs_filtered.withColumn(
                "battleTime", convertDT("battleTime")
            )
            # Transform java.util.GregorianCalendar low level date format to Pyspark
            pattern = r"(?:.*)YEAR=(\d+).+?MONTH=(\d+).+?DAY_OF_MONTH=(\d+)" \
                      r".+?HOUR=(\d+).+?MINUTE=(\d+).+?SECOND=(\d+).+"
            battlelogs_filtered = battlelogs_filtered.withColumn(
                "battleTime",
                f.regexp_replace("battleTime", pattern, "$1-$2-$3 $4:$5:$6").cast(
                    "timestamp"
                ),
            ).withColumn("battleTime", f.date_format("battleTime", format="yyyy-MM-dd"))

        # List to allocate DFs
        cohort_selection = []
        # Classify sample cohort
        cohort_num = 1
        # Loop over each one of the parameters to subset based on time ranges
        for date_range in parameters["cohort_time_range"]:
            cohort_range = battlelogs_filtered.filter(
                (f.col("battleTime") > literal_eval(date_range)[0])
                & (f.col("battleTime") < literal_eval(date_range)[1])
            )
            cohort_range = cohort_range.withColumn("cohort", f.lit(cohort_num))
            cohort_num += 1
            cohort_selection.append(cohort_range)
        # Reduce all dataframe to overwrite original
        battlelogs_filtered = reduce(DataFrame.unionAll, cohort_selection)

    else:
        log.warning(
            "Verify to have a minimum of one time range defined for your cohort. "
            "Check the parameters defined"
        )
        raise

    # Create hierarchical ID (in case the code breaks increase default-parl to 100)
    battlelogs_filtered = battlelogs_filtered.withColumn(
        "battlelog_id", f.monotonically_increasing_id()
    )

    return battlelogs_filtered
