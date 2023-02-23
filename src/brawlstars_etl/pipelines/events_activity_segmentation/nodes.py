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
import pyspark.sql.functions as f
import pyspark.sql.types as t

def _group_exploder_solo(event_solo_data: pyspark.sql.DataFrame,
                        standard_columns: list
) -> pyspark.sql.DataFrame:
    '''Helper function to mine and extract player session teammate information
     from dictionary-formatted columns'''

    # Explode list of player dictionaries as StringTypes
    event_solo_data = event_solo_data.withColumn('battle_players', f.explode('battle_players'))

    # Convert dictionary of players from StringTypes to MapTypes
    MapStructure = t.MapType(t.StringType(), t.StringType())
    event_solo_data = event_solo_data.withColumn('battle_players', f.from_json('battle_players', MapStructure))

    # Extract players tag and brawler name
    event_solo_data = (event_solo_data.withColumn('tag', f.col('battle_players').getItem('tag'))
                                       .withColumn('brawler', f.col('battle_players').getItem('brawler'))
                       )
    # Convert dictionary with brawlers as StringTypes to MapTypes
    event_solo_data = event_solo_data.withColumn('brawler', f.from_json('brawler', MapStructure))
    event_solo_data = event_solo_data.withColumn('brawler', f.col('brawler').getItem('name'))

    # Convert to flatten dataframe
    event_solo_data = event_solo_data.groupBy(standard_columns).agg(f.collect_list('tag').alias('players_tag'),
                                                                    f.collect_list('brawler').alias('players_brawler'))

    return event_solo_data


def _group_exploder_duo(event_solo_duo: pyspark.sql.DataFrame,
                        standard_columns: list
) -> pyspark.sql.DataFrame:
    '''Helper function to mine and extract duos information
     from dictionary-formatted columns'''

    # Explode all teams (duos) as StringTypes
    event_solo_duo = event_solo_duo.withColumn('battle_teams', f.explode('battle_teams'))

    # Convert dictionary of players from StringTypes to Array of MapTypes
    MapStructure = t.MapType(t.StringType(), t.StringType())
    event_solo_duo = event_solo_duo.withColumn('battle_teams', f.from_json('battle_teams', t.ArrayType(MapStructure)))

    # Separate and concatenate the 2 player tags
    event_solo_duo = (event_solo_duo.withColumn('team_player_1', f.col('battle_teams').getItem(0))
                                    .withColumn('team_player_2', f.col('battle_teams').getItem(1))
                                    .withColumn('team_player_1_tag', f.col('team_player_1').getItem('tag'))
                                    .withColumn('team_player_2_tag', f.col('team_player_2').getItem('tag'))
                                    .withColumn('player_duos_exploded',
                                                f.array('team_player_1_tag', 'team_player_2_tag'))
                      )
    # Separate and concatenate the 2 player's brawlers
    event_solo_duo = (event_solo_duo.withColumn('team_player_1_brawler', f.col('team_player_1').getItem('brawler'))
                                    .withColumn('team_player_2_brawler', f.col('team_player_2').getItem('brawler'))
                                    .withColumn('team_player_1_brawler',
                                                f.from_json('team_player_1_brawler', MapStructure))
                                    .withColumn('team_player_2_brawler',
                                                f.from_json('team_player_2_brawler', MapStructure))
                                    .withColumn('team_player_1_brawler_name',
                                                f.col('team_player_1_brawler').getItem('name'))
                                    .withColumn('team_player_2_brawler_name',
                                                f.col('team_player_2_brawler').getItem('name'))
                                    .withColumn('brawler_duos_exploded',
                                                f.array('team_player_1_brawler_name', 'team_player_2_brawler_name'))
                      )
    # Convert to flatten dataframe
    event_solo_duo = (event_solo_duo.groupBy(standard_columns)
                      .agg(f.collect_list('player_duos_exploded').alias('player_duos'),
                           f.collect_list('brawler_duos_exploded').alias('brawler_duos'))
                      )
    return event_solo_duo


def _group_exploder_3v3(event_3v3_data: pyspark.sql.DataFrame,
                        standard_columns: list
) -> pyspark.sql.DataFrame:
    '''Helper function to mine and extract 3v3 sessions information
     from dictionary-formatted columns'''

    # Explode the two teams (3v3) as StringTypes
    event_3v3_data = event_3v3_data.withColumn('battle_teams', f.explode('battle_teams'))

    # Convert dictionary of players from StringTypes to Array of MapTypes
    MapStructure = t.MapType(t.StringType(), t.StringType())
    event_3v3_data = event_3v3_data.withColumn('battle_teams', f.from_json('battle_teams', t.ArrayType(MapStructure)))

    # Separate and concatenate the 3 player tags
    event_3v3_data = (event_3v3_data.withColumn('team_player_1', f.col('battle_teams').getItem(0))
                                    .withColumn('team_player_2', f.col('battle_teams').getItem(1))
                                    .withColumn('team_player_3', f.col('battle_teams').getItem(2))
                                    .withColumn('team_player_1_tag', f.col('team_player_1').getItem('tag'))
                                    .withColumn('team_player_2_tag', f.col('team_player_2').getItem('tag'))
                                    .withColumn('team_player_3_tag', f.col('team_player_3').getItem('tag'))
                                    .withColumn('player_trios_exploded',
                                                f.array('team_player_1_tag', 'team_player_2_tag', 'team_player_3_tag'))
                      )
    # Separate and concatenate the 3 player's brawlers
    event_3v3_data = (event_3v3_data.withColumn('team_player_1_brawler', f.col('team_player_1').getItem('brawler'))
                                    .withColumn('team_player_2_brawler', f.col('team_player_2').getItem('brawler'))
                                    .withColumn('team_player_3_brawler', f.col('team_player_3').getItem('brawler'))
                                    .withColumn('team_player_1_brawler',
                                                f.from_json('team_player_1_brawler', MapStructure))
                                    .withColumn('team_player_2_brawler',
                                                f.from_json('team_player_2_brawler', MapStructure))
                                    .withColumn('team_player_3_brawler',
                                                f.from_json('team_player_3_brawler', MapStructure))
                                    .withColumn('team_player_1_brawler_name',
                                                f.col('team_player_1_brawler').getItem('name'))
                                    .withColumn('team_player_2_brawler_name',
                                                f.col('team_player_2_brawler').getItem('name'))
                                    .withColumn('team_player_3_brawler_name',
                                                f.col('team_player_3_brawler').getItem('name'))
                                    .withColumn('brawler_trios_exploded',
                                                f.array('team_player_1_brawler_name',
                                                        'team_player_2_brawler_name',
                                                        'team_player_3_brawler_name'))
                      )
    # Convert to flatten dataframe
    event_3v3_data = (event_3v3_data.groupBy(standard_columns)
                      .agg(f.collect_list('player_trios_exploded').alias('player_trios'),
                           f.collect_list('brawler_trios_exploded').alias('brawler_trios'))
                      )

    return event_3v3_data

def battlelogs_deconstructor(battlelogs_filtered: pyspark.sql.DataFrame,
                            parameters: Dict
) -> (pyspark.sql.DataFrame, pyspark.sql.DataFrame):
    # Call | Create Spark Session
    spark = SparkSession.builder.getOrCreate()

    # TODO: read more https://brawlstars.fandom.com/wiki/Hot_Zone
    # TODO: See how to extract items here https://stackoverflow.com/questions/64476394/how-to-parse-and-explode-a-list-of-dictionaries-stored-as-string-in-pyspark

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
        event_duo_data = _group_exploder_duo(event_duo_data, parameters['standard_columns'])
    else:
        log.warning("Duo Event modes not defined or not found according to parameter list")
        event_duo_data = spark.createDataFrame([], schema=t.StructType([]))

    log.info("Deconstructing 3 vs 3 Events")
    if parameters['event_3v3'] and isinstance(parameters['event_3v3'], list):
        event_3v3_data = battlelogs_filtered.filter(f.col('event_mode').isin(parameters['event_3v3']))
        event_3v3_data = _group_exploder_3v3(event_3v3_data, parameters['standard_columns'])
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