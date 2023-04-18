"""
This is a boilerplate pipeline 'metadata_request_preprocess'
generated using Kedro 0.18.4
"""
import pandas as pd
import brawlstats
import numpy as np
import time
import logging
from typing import Any, Dict
import asyncio
import pyspark.sql
from pyspark.sql import SparkSession

# To load the configuration (https://kedro.readthedocs.io/en/stable/kedro_project_setup/configuration.html#credentials)
from kedro.config import ConfigLoader
from kedro.framework.project import settings

conf_loader_local = ConfigLoader(conf_source=settings.CONF_SOURCE, env="local")
conf_credentials = conf_loader_local["credentials"]

log = logging.getLogger(__name__)


def players_info_request(
    player_tags_txt: str, parameters: Dict[str, Any]
) -> pyspark.sql.DataFrame:
    """
    Extracts metadata for players with given tags from Brawlstars API asynchronously,
    using a list of futures objects that consist of async threads. This is done to
    overcome blocking call limitations of the api_request sub_module.
    Args:
        player_tags_txt: List of player tags
        parameters: Additional parameters to pass in the API request
    Returns:
        A structured DataFrame containing metadata for all players.
    """
    # Retrieve the API key and verify that it exists
    API_KEY = conf_credentials.get("brawlstars_api", None).get("API_KEY", None)
    try:
        assert API_KEY is not None
    except AssertionError:
        log.info(
         "No API key has been defined. Request one at https://developer.brawlstars.com/"
        )

    # Create a client object using the Brawlstats API wrapper and prevent rate limiting
    client = brawlstats.Client(token=API_KEY)

    # Split the player tags into a list
    player_tags_txt = player_tags_txt.split(",")

    def api_request(tag: str) -> pd.DataFrame:
        """Requests player data from the Brawl Stars API and returns it in a
        structured pandas DataFrame."""
        try:
            # Request player data from the Brawl Stars API
            player_metadata = client.get_player(tag).raw_data
            # Normalize the player data to a structured format
            player_metadata_structured = pd.json_normalize(player_metadata)
            # Rename the 'tag' column to 'player_id'
            player_metadata_structured.rename(
                {"tag": "player_id"}, axis=1, inplace=True
            )
        except:
            # If no metadata was extracted for the player, return an empty DataFrame
            log.info(f"No metadata extracted for player {tag}")
            player_metadata_structured = pd.DataFrame()
            pass
        return player_metadata_structured

    async def api_request_async(tag: str) -> pd.DataFrame:
        """
        Transforms non-async request function to async coroutine, which creates a
        future object per API request.
        The Coroutine contains a blocking call that won't return a log until it's
        complete. So, to run concurrently, await the thread and not the coroutine by
        using this method.
        """
        return await asyncio.to_thread(api_request, tag)

    async def spawn_request(player_tags: list) -> pd.DataFrame:
        """Asynchronously requests player metadata for multiple tags"""
        start = time.time()
        log.info(f"Player info request process started")
        # Create a list of coroutines (Task Objects) and schedule their execution
        requests_tasks = [
            asyncio.create_task(api_request_async(tag)) for tag in player_tags
        ]
        # Wait for tasks to complete and retrieve the player metadata as a list DFs
        player_metadata_list = await asyncio.gather(*requests_tasks)
        # Concatenate all DataFrames into one DataFrame
        player_metadata = pd.concat(player_metadata_list, ignore_index=True)
        elapsed_time = time.time() - start
        log.info(f"Finished requesting player info in {elapsed_time:.2f} seconds.")
        return player_metadata

    def activate_request(n: int = None) -> pd.DataFrame:
        """
        Retrieve player metadata by sending requests to an API using asyncio.
        If n is provided, only a sample of players will be retrieved for testing
        purposes. Otherwise, the entire batch of players will be retrieved to
        prevent being rate-limited by the API. The player metadata is concatenated
        into a single DataFrame and returned.
        """
        player_metadata = pd.DataFrame()
        if n:
            # Retrieve metadata for a sample of players
            player_metadata = asyncio.run(spawn_request(player_tags_txt[:n]))
        else:
            # Retrieve metadata for entire batch of players
            split_tags = np.array_split(player_tags_txt, len(player_tags_txt) / 10)
            for batch in split_tags:
                player_metadata_tmp = asyncio.run(spawn_request(batch))
                # Concatenate player metadata for each batch into a single DataFrame
                try:
                    player_metadata = pd.concat(
                        [player_metadata, player_metadata_tmp],
                        axis=0,
                        ignore_index=True,
                    )
                except:
                    pass

        return player_metadata

    # Activate a request for player metadata with a limit specified in parameters
    player_metadata = activate_request(n=parameters["metadata_limit"])

    # Replace dots in column names with underscores
    player_metadata.columns = [
        col_name.replace(".", "_") for col_name in player_metadata.columns
    ]

    # Check that the data request was not affected by concurrency issues
    try:
        assert not player_metadata.empty
    except AssertionError:
        # Log an error message if no metadata was extracted
        log.info("No metadata was extracted. Please check your client connection.")

    return player_metadata


def metadata_preparation(
    player_metadata: pd.DataFrame, parameters: Dict
) -> pyspark.sql.DataFrame:
    """
    Preprocesses player metadata for analysis with Spark, adapting its format to match
    a given DDL schema and removing unnecessary columns.
    Args:
        player_metadata: A structured DataFrame with information for all players.
        parameters: A dictionary containing a DDL schema.
    Returns:
        A preprocessed Pyspark DataFrame with rows relevant for analysis.
    """
    # Call | Create Spark Session
    spark = SparkSession.builder.getOrCreate()

    # Load player metadata into a Spark DataFrame and validate against a DDL schema
    try:
        players_metadata_prepared = spark.createDataFrame(
            data=player_metadata, schema=parameters["metadata_schema"][0]
        )
    except TypeError:
        log.warning(
            "Type error on the DDL schema for the player metadata,"
            'check "metadata_schema" on the node parameters'
        )
        raise

    # Drop seasonal columns & non-descriptive columns ('brawlers' is linked to the
    # analysis type)
    players_metadata_prepared = players_metadata_prepared.drop(
        "name",
        "nameColor",
        "isQualifiedFromChampionshipChallenge",
        "brawlers",
        "icon_id",
        "club_tag",
        "club_name",
        "bestTimeAsBigBrawler",
        'highestPowerPlayPoints',
    )

    return players_metadata_prepared.toPandas()
