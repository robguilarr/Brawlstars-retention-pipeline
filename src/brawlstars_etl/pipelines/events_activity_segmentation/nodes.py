"""
This is a boilerplate pipeline 'events_activity_segmentation'
generated using Kedro 0.18.4
"""
# Logging
import time
import logging
log = logging.getLogger(__name__)
from typing import Dict
# Spark SQL API
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t

def _group_exploder_solo(event_solo_data: pyspark.sql.DataFrame,
                        standard_columns: list
) -> pyspark.sql.DataFrame:
    '''Helper function to mine and extract player session teammate information from dictionary-formatted columns'''
    # TODO: See how to extract items here https://stackoverflow.com/questions/64476394/how-to-parse-and-explode-a-list-of-dictionaries-stored-as-string-in-pyspark

    # Explode list of dictionaries as StringTypes
    event_solo_data = event_solo_data.withColumn('battle_players', f.explode('battle_players'))
    # Convert dictionary of players as StringTypes to MapTypes
    event_solo_data = event_solo_data.withColumn('battle_players', f.from_json('battle_players',
                                                                               t.MapType(t.StringType(), t.StringType())
                                                                               )
                                                 )
    # Extract players tag and brawler name
    event_solo_data = (event_solo_data.withColumn('tag',
                                                    f.col('battle_players').getItem('tag')
                                                       )
                                       .withColumn('brawler',
                                                    f.col('battle_players').getItem('brawler')
                                                       )
                       )
    # Convert dictionary with brawlers as StringTypes to MapTypes
    event_solo_data = event_solo_data.withColumn('brawler', f.from_json('brawler',
                                                                        t.MapType(t.StringType(), t.StringType())
                                                                        )
                                                 )
    event_solo_data = event_solo_data.withColumn('brawler',
                                                  f.col('brawler').getItem('name')
                                                  )
    # Convert to flatten dataframe
    event_solo_data = event_solo_data.groupBy(standard_columns).agg(f.collect_set('tag').alias('players_tag'),
                                                                    f.collect_set('brawler').alias('players_brawler'))

    #event_solo_data.show(truncate=False)
    return event_solo_data


def battlelogs_deconstructor(battlelogs_filtered: pyspark.sql.DataFrame,
                            parameters: Dict
) -> (pyspark.sql.DataFrame, pyspark.sql.DataFrame):
    # Call | Create Spark Session
    spark = SparkSession.builder.getOrCreate()

    # TODO: We always have to take battle_teams and figure out how to separate by event_mode==battle_mode after exploding battle_teams
    log.info("Deconstructing Solo Events")
    if parameters['event_solo'] and isinstance(parameters['event_solo'], list):
        event_solo_data = battlelogs_filtered.filter(f.col('event_mode').isin(parameters['event_solo']))
        event_solo_data = _group_exploder_solo(event_solo_data, parameters['standard_columns'])
    else:
        log.warning("Solo Event modes not defined or not found according to parameter list")
        event_solo_data = spark.createDataFrame([], schema=t.StructType([]))

    log.info("Deconstructing Duo Events")
    if parameters['event_duo'] and isinstance(parameters['event_duo'], list):
        event_duo_data = battlelogs_filtered.filter(f.col('event_mode').isin(parameters['event_duo']))
    else:
        log.warning("Duo Event modes not defined or not found according to parameter list")
        event_duo_data = spark.createDataFrame([], schema=t.StructType([]))

    log.info("Deconstructing 3 vs 3 Events")
    if parameters['event_3v3'] and isinstance(parameters['event_3v3'], list):
        event_3v3_data = battlelogs_filtered.filter(f.col('event_mode').isin(parameters['event_3v3']))
    else:
        log.warning("3 vs 3 Event modes not defined or not found according to parameter list")
        event_3v3_data = spark.createDataFrame([], schema=t.StructType([]))

    log.info("Deconstructing Special Events")
    if parameters['event_special'] and isinstance(parameters['event_special'], list):
        event_special_data = battlelogs_filtered.filter(f.col('event_mode').isin(parameters['event_special']))
    else:
        log.warning("Special Event modes not defined or not found according to parameter list")
        event_special_data = spark.createDataFrame([], schema=t.StructType([]))

    # From here we also need to have the missing gametags as output to be used by 'Players Info Request Node'

    # Then a deconstructor node for interactivity (between player and hero - s) for each one of the event type
    # (do a modular pipeline). Feed hero info with API request (but with its own pipeline to avoid doing extra
    # requests when refreshing this one). Also feed this one with players info, and don't forget a boolean to
    # define if the team

    # Star player is selected as https://help.supercellsupport.com/brawl-stars/en/articles/star-player.html

    return event_solo_data, event_duo_data, event_3v3_data, event_special_data