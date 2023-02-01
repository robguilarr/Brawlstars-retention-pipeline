"""
This is a boilerplate pipeline 'battlelogs_request_preprocess'
generated using Kedro 0.18.4
"""
# General dependencies
import pandas as pd
import brawlstats
# Parameters definitions
from typing import Any, Dict, Tuple
from ast import literal_eval
from functools import reduce

import pyspark.sql
# To load the configuration (https://kedro.readthedocs.io/en/stable/kedro_project_setup/configuration.html#credentials)
from kedro.config import ConfigLoader
from kedro.framework.project import settings
conf_loader_local = ConfigLoader(conf_source= settings.CONF_SOURCE, env= 'local')
conf_credentials = conf_loader_local['credentials']
# Logging
import time
import logging
log = logging.getLogger(__name__)
# Async processes
import asyncio
# Spark SQL API
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def battlelogs_request(player_tags: str) -> pd.DataFrame:
    '''
    Extracts Battlelogs from Brawlstars API by executing an Async Event Loop over a list of futures objects. These are
    made of task objects built of Async threads due blocking call limitations of api_request sub_module.
    Args:
        player_tags: PLayer tag list
    Returns:
        All players battlelogs concatenated into a structured Dataframe
    '''
    # Get key and validate it exists
    API_KEY = conf_credentials.get('brawlstars_api', None).get('API_KEY', None)
    try:
        assert API_KEY != None
    except AssertionError:
        log.info("No API key has been defined. Request one at https://developer.brawlstars.com/")

    # Create client object from brawlstats API wrapper, be aware of preventing the rate limit for huge requests,
    # review prevent_ratelimit in the source code
    client = brawlstats.Client(token=API_KEY)

    # Create list of player tags, from catalog
    player_tags = player_tags.split(',')

    def api_request(tag: str) -> pd.DataFrame:
        '''Request battlelogs from the Brawl Stars API and give a strutured format'''
        try:
            # Extract list of 25 most recent session logs
            player_battle_logs = client.get_battle_logs(tag).raw_data
            # Normalize data in structured format
            player_battle_logs_structured = pd.json_normalize(player_battle_logs)
            player_battle_logs_structured['player_id'] = tag
        except:
            log.info(f"No Battlelog extracted for player {tag}")
            player_battle_logs_structured = pd.DataFrame()
            pass
        return player_battle_logs_structured

    async def api_request_async(tag: str) -> pd.DataFrame:
        '''
        Transform non-sync request function to async coroutine, which creates
        a future object by API request.
        The Coroutine contains a blocking call that won't return a log until it's complete. So,
        to run concurrently, await the thread and not the coroutine by using this method.
        '''
        return await asyncio.to_thread(api_request, tag)

    async def spawn_request(player_tags: list) -> pd.DataFrame:
        '''Use gathering to request battlelogs as async tasks objects, made of coroutines'''
        start = time.time()
        log.info(f"Battlelogs request process started")
        # Comprehensive list of coroutines as Task Objects, whom will be already scheduled its execution
        requests_tasks = [asyncio.create_task(api_request_async(tag)) for tag in player_tags]
        # Future Object: List of battlelogs as Dataframes
        battlelogs_data_list = await asyncio.gather(*requests_tasks)
        # When all tasks all executed, concat all dataframes into one
        battlelogs_data = pd.concat(battlelogs_data_list, ignore_index=True)
        log.info(f"Battlelogs request process Finished in {time.time() - start} seconds")
        return battlelogs_data

    # Run the events-loop
    battlelogs_data = asyncio.run(spawn_request(player_tags[:20]))

    # Replace dots in column names
    battlelogs_data.columns = [col_name.replace('.','_') for col_name in battlelogs_data.columns]

    # Validate concurrency didn't affect the data request
    try:
        assert not battlelogs_data.empty
    except AssertionError:
        log.info("No Battlelogs were extracted. Please check your Client Connection")

    return battlelogs_data


def battlelogs_filter(raw_battlelogs: pd.DataFrame,
                          parameters: Dict[str, Any]
) -> pyspark.sql.DataFrame:

    # Create Spark dataframe based on pandas parquet
    spark = SparkSession.builder.getOrCreate()

    # Ingest battlelogs data and validate against DDL schema
    try:
        battlelogs_filtered = spark.createDataFrame(data = raw_battlelogs,
                                                  schema = parameters['raw_battlelogs_schema'][0])
    except TypeError:
        log.warning('Type error on the DDL schema for the battlelogs,'
                    'check "raw_battlelogs_schema" on the node parameters')
        raise

    # Filter date based on timestamp, separated cohort can be extracted to exclude specific dates
    if parameters['cohort_time_range']:
        if 'battleTime' not in battlelogs_filtered.columns:
            raise ValueError('Check dataframe contains "battleTime" column')
        # List to allocate DFs
        cohort_selection = []
        # Classify sample cohort
        cohort_num = 1
        # Loop over each on of the time ranges to subset based on parameters
        for date_range in parameters['cohort_time_range']:
            cohort_range = battlelogs_filtered.filter(
                (f.col('battleTime') > literal_eval(date_range)[0])
                & (f.col('battleTime') > literal_eval(date_range)[1])
            )
            cohort_range = cohort_range.withColumn('cohort', f.lit(cohort_num))
            cohort_num += 1
            cohort_selection.append(cohort_range)
        # Reduce all dataframe to overwrite original
        battlelogs_filtered = reduce(DataFrame.unionAll, cohort_selection)

    return battlelogs_filtered

def battlelogs_deconstructor():
    data1 = pd.DataFrame()
    data2 = pd.DataFrame()
    return 'a'