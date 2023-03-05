"""
This is a boilerplate pipeline 'events_activity_segmentation'
generated using Kedro 0.18.4
"""
# Logging
import logging
log = logging.getLogger(__name__)
from typing import Dict
# Spark SQL API
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window
# Aggregation functions
from functools import reduce


def _group_exploder_solo(event_solo_data: pyspark.sql.DataFrame,
                        standard_columns: list
) -> pyspark.sql.DataFrame:
    '''Helper function to subset solo-events information
     from dictionary-formatted columns'''

    try:
        # Explode list of player dictionaries as StringTypes
        event_solo_data = event_solo_data.withColumn('battle_players', f.explode('battle_players'))

        # Convert dictionary of players from StringTypes to MapTypes
        MapStructure = t.MapType(t.StringType(), t.StringType())
        event_solo_data = event_solo_data.withColumn('battle_players', f.from_json('battle_players', MapStructure))

        # Extract players tags
        event_solo_data = (event_solo_data.withColumn('tag', f.col('battle_players').getItem('tag'))
                                           .withColumn('brawler', f.col('battle_players').getItem('brawler'))
                           )
        # Convert dictionary with brawlers as StringTypes to MapTypes and extract brawler
        event_solo_data = event_solo_data.withColumn('brawler', f.from_json('brawler', MapStructure))
        event_solo_data = event_solo_data.withColumn('brawler', f.col('brawler').getItem('name'))

        # Convert to flatten dataframe
        event_solo_data = (event_solo_data.groupBy(standard_columns)
                                          .agg(f.collect_list('tag').alias('players_collection'),
                                               f.collect_list('brawler').alias('brawlers_collection')))
    except Exception as e:
        log.exception(e)
        log.warning("-- Exploder broken: Check 'battle_players' column has a consistent structure according API specs --")
        standard_columns.extend(['battle_players'])
        event_solo_data = (event_solo_data.select(*standard_columns)
                                          .withColumn('players_collection',
                                                      f.lit("['null']").cast(t.ArrayType(t.StringType())))
                                          .withColumn('brawlers_collection',
                                                      f.lit("['null']").cast(t.ArrayType(t.StringType())))
                           )
    return event_solo_data

def _group_exploder_duo(event_duo_data: pyspark.sql.DataFrame,
                        standard_columns: list
) -> pyspark.sql.DataFrame:
    '''Helper function to subset duo-events information
     from dictionary-formatted columns'''

    try:
        # Explode all teams (list of duos) as StringTypes
        event_duo_data = event_duo_data.withColumn('battle_teams', f.explode('battle_teams'))

        # Convert dictionary of players from StringTypes to Array of MapTypes
        MapStructure = t.MapType(t.StringType(), t.StringType())
        event_duo_data = event_duo_data.withColumn('battle_teams', f.from_json('battle_teams', t.ArrayType(MapStructure)))

        # Separate and concatenate the 2 player's tags
        event_duo_data = (event_duo_data.withColumn('team_player_1', f.col('battle_teams').getItem(0))
                                        .withColumn('team_player_2', f.col('battle_teams').getItem(1))
                                        .withColumn('team_player_1_tag', f.col('team_player_1').getItem('tag'))
                                        .withColumn('team_player_2_tag', f.col('team_player_2').getItem('tag'))
                                        .withColumn('player_duos_exploded',
                                                    f.array('team_player_1_tag', 'team_player_2_tag'))
                          )
        # Separate and concatenate the 2 player's brawlers
        event_duo_data = (event_duo_data.withColumn('team_player_1_brawler', f.col('team_player_1').getItem('brawler'))
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
        event_duo_data = (event_duo_data.groupBy(standard_columns)
                                        .agg(f.collect_list('player_duos_exploded').alias('players_collection'),
                                             f.collect_list('brawler_duos_exploded').alias('brawlers_collection'))
                          )
    except Exception as e:
        log.exception(e)
        log.warning("-- Exploder broken: Check 'battle_teams' column has a consistent structure according API specs --")
        standard_columns.extend(['battle_teams'])
        event_duo_data = (event_duo_data.select(*standard_columns)
                                        .withColumn('players_collection',
                                                    f.lit("['null']").cast(t.ArrayType(t.StringType())))
                                        .withColumn('brawlers_collection',
                                                    f.lit("['null']").cast(t.ArrayType(t.StringType())))
                           )
    return event_duo_data

def _group_exploder_3v3(event_3v3_data: pyspark.sql.DataFrame,
                        standard_columns: list
) -> pyspark.sql.DataFrame:
    '''Helper function to subset 3v3 events information
     from dictionary-formatted columns'''

    try:
        # Explode the two teams (3v3) as StringTypes
        event_3v3_data = event_3v3_data.withColumn('battle_teams', f.explode('battle_teams'))

        # Convert dictionary of players from StringTypes to Array of MapTypes
        MapStructure = t.MapType(t.StringType(), t.StringType())
        event_3v3_data = event_3v3_data.withColumn('battle_teams', f.from_json('battle_teams', t.ArrayType(MapStructure)))

        # Separate and concatenate the 3 player's tags
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
                                        .agg(f.collect_list('player_trios_exploded').alias('players_collection'),
                                             f.collect_list('brawler_trios_exploded').alias('brawlers_collection'))
                          )
    except Exception as e:
        log.exception(e)
        log.warning("-- Exploder broken: Check 'battle_teams' column has a consistent structure according API specs --")
        standard_columns.extend(['battle_teams'])
        event_3v3_data = (event_3v3_data.select(*standard_columns)
                                        .withColumn('players_collection',
                                                    f.lit("['null']").cast(t.ArrayType(t.StringType())))
                                        .withColumn('brawlers_collection',
                                                    f.lit("['null']").cast(t.ArrayType(t.StringType())))
                           )
    return event_3v3_data

def _group_exploder_special(event_special_data: pyspark.sql.DataFrame,
                            standard_columns: list
) -> pyspark.sql.DataFrame:
    '''Helper function to subset special events information
     from dictionary-formatted columns
     '''

    try:
        # Explode the list of team players as StringTypes
        event_special_data = event_special_data.withColumn('battle_players', f.explode('battle_players'))

        # Convert dictionary of players from StringTypes to Array of MapTypes
        MapStructure = t.MapType(t.StringType(), t.StringType())
        event_special_data = event_special_data.withColumn('battle_players', f.from_json('battle_players', MapStructure))

        # Separate and concatenate player's tags and brawlers
        event_special_data = (event_special_data.withColumn('team_player_tag', f.col('battle_players').getItem('tag'))
                                                .withColumn('team_player_brawler', f.col('battle_players').getItem('brawler'))
                                                .withColumn('team_player_brawler', f.from_json('team_player_brawler', MapStructure))
                                                .withColumn('team_player_brawler_name', f.col('team_player_brawler').getItem('name'))
                              )
        # Convert to flatten dataframe
        standard_columns.extend(['battle_bigBrawler_tag', 'battle_bigBrawler_brawler_name'])
        event_special_data = (event_special_data.groupBy(standard_columns)
                                                .agg(f.collect_list('team_player_tag').alias('players_collection'),
                                                     f.collect_list('team_player_brawler_name').alias('brawlers_collection'))
                              )
    except Exception as e:
        log.exception(e)
        log.warning("-- Exploder broken: Check 'battle_players' column has a consistent structure according API specs --")
        standard_columns.extend(['battle_players'])
        event_special_data = (event_special_data.select(*standard_columns)
                                                .withColumn('players_collection',
                                                            f.lit("['null']").cast(t.ArrayType(t.StringType())))
                                                .withColumn('brawlers_collection',
                                                            f.lit("['null']").cast(t.ArrayType(t.StringType())))
                           )
    return event_special_data


def battlelogs_deconstructor(battlelogs_filtered: pyspark.sql.DataFrame,
                            parameters: Dict
) -> (pyspark.sql.DataFrame, pyspark.sql.DataFrame,
      pyspark.sql.DataFrame, pyspark.sql.DataFrame):
    '''
    Disassembly (Explosion) of player group records from raw JSON formats, to extract combinations of players and
    brawlers from the same team or from opponents, this is for each of the sessions.
    Each of the 'exploders' is parameterized by the user, according to the number of players needed for each type
    of event.
    Args:
        battlelogs_filtered: Filtered Pyspark DataFrame containing only cohorts and features required
        parameters: Event types defined by the user to include in the subset process
    Returns:
        Four Pyspark dataframes containing only one event type each
    '''
    # Call | Create Spark Session
    spark = SparkSession.builder.getOrCreate()

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
        event_special_data = _group_exploder_special(event_special_data, parameters['standard_columns'])
    else:
        log.warning("Special Event modes not defined or not found according to parameter list")
        event_special_data = spark.createDataFrame([], schema=t.StructType([]))

    return event_solo_data, event_duo_data, event_3v3_data, event_special_data


@f.udf(returnType= t.IntegerType())
def retention_metric(days_activity_list, day):
    '''User Defined Function to return the user-retention
     given the definition given by the parameter'''
    # Subset columns of days needed
    days_activity = days_activity_list[:day + 1]
    # When day is 0 return the same value
    if day == 0:
        if days_activity[0] == 1:
            return 1
        else:
            return 0
    # Evaluate the day of retention is on the data, otherwise return 0
    elif day > len(days_activity):
        return 0
    else:
        # Evaluate user installed the app and return on the specific day
        if days_activity[0] == 1 and days_activity[-1] == 1:
            return 1
        else:
            return 0

@f.udf(returnType= t.IntegerType())
def sessions_sum(daily_sessions_list, day):
    '''User Defined Function to return the total of
     sessions accumulated, limiting the ranges by the
     parameter defined by the user'''
    # Subset columns of days needed
    daily_sessions = daily_sessions_list[:day + 1]
    # Sum the total of sessions in the given range
    total = 0
    for i in range(len(daily_sessions)):
        if daily_sessions[i] != None:
            total += int(daily_sessions[i])
    return total

def activity_transformer(battlelogs_filtered: pyspark.sql.DataFrame,
                         parameters: Dict
) -> pyspark.sql.DataFrame:
    '''
    Converts the filtered battle logs into a wrapped format data frame, taking a set
    of parameters such as days, to extract the retention (and the number of sessions).
    Performance detail: The Cohort transformation skips exhaustive intermediate transformations,
    such as 'Sort', since the user can insert many cohorts as they occur in the preprocessing stage,
    causing excessive partitioning.
    Args:
        battlelogs_filtered: Filtered Pyspark DataFrame containing only cohorts and features required for the study
        parameters: Frequency of the cohort and days to extract for the output
    Returns:
        Pyspark dataframe with retention metrics and n-sessions at the player level of granularity.
    '''
    # Aggregate user activity data to get daily number of sessions
    user_activity = (battlelogs_filtered.select('cohort','battleTime','player_id')
                                        .groupBy('cohort','battleTime','player_id').count()
                                        .withColumnRenamed('count','daily_sessions')
                     )
    # Validate Cohort Frequency, default to 'daily'
    if parameters['cohort_frequency'] and isinstance(parameters['cohort_frequency'], str):
        # Construct the cohort on a weekly basis
        if parameters['cohort_frequency'] == 'weekly':
            time_freq = 'weekly_battleTime'
            user_activity = user_activity.withColumn(time_freq,
                                                     f.date_sub(f.next_day('battleTime', 'Monday'), 7))
        # Construct the cohort on a monthly basis
        elif parameters['cohort_frequency'] == 'monthly':
            time_freq = 'monthly_battleTime'
            user_activity = user_activity.withColumn(time_freq,
                                                     f.trunc('battleTime', 'month'))
        # Construct the cohort on a daily basis (default)
        else:
            time_freq = 'battleTime'
    else:
        time_freq = 'battleTime'

    # Create a window by each one of the player's window
    player_window = Window.partitionBy(['player_id'])

    # Find the first logged date per player in the cohort
    user_activity = user_activity.withColumn('first_log',
                                             f.min(time_freq).over(player_window))

    # Find days passed to see player return
    user_activity = user_activity.withColumn('days_to_return',
                                             f.datediff(time_freq,'first_log'))

    # Subset required columns and add counter variable to aggregate number of player further
    user_activity = (user_activity.select('cohort','player_id','first_log','days_to_return','daily_sessions')
                                    .withColumn('player_count', f.lit(1)))

    # List cohorts, iterate over them and append them to the final output
    cohort_list = [row.cohort for row in user_activity.select('cohort').distinct().collect()]
    output_range = []

    for cohort in cohort_list:
        # Filter only data of the cohort in process
        tmp_user_activity = user_activity.filter(f.col('cohort') == cohort)

        # Pivot data from long format to wide
        tmp_user_activity = (tmp_user_activity.groupBy(['player_id', 'first_log'])
                                                .pivot('days_to_return')
                                                .agg(f.sum('player_count').alias('day'),
                                                     f.sum('daily_sessions').alias('day_sessions')))

        # Extract daily activity columns, save as arrays of column objects
        day_cohort_col = tmp_user_activity.select(tmp_user_activity.colRegex("`.+_day$`")).columns
        day_cohort_col = f.array(*map(f.col, day_cohort_col))

        # Extract daily sessions counter columns, save as arrays of column objects
        day_sessions_col = tmp_user_activity.select(tmp_user_activity.colRegex("`.+_day_sessions$`")).columns
        day_sessions_col = f.array(*map(f.col, day_sessions_col))

        # Empty list to allocate final columns
        cols_retention = []

        # Produce retention metrics and session counters
        for day in parameters['retention_days']:
            # Define column names
            DDR = f'D{day}R'
            DDTS = f'D{day}_Sessions'
            # Obtain retention for a given day
            tmp_user_activity = tmp_user_activity.withColumn(DDR, retention_metric(day_cohort_col, f.lit(day)))
            # Obtain total of sessions until given day
            tmp_user_activity = (tmp_user_activity.withColumn(DDTS, sessions_sum(day_sessions_col, f.lit(day)))
                                          .withColumn(DDTS, f.when(f.col(DDR) != 1, 0).otherwise(f.col(DDTS))))
            # Append final columns
            cols_retention.append(DDR)
            cols_retention.append(DDTS)

        # Final formatting
        standard_columns = ['player_id','first_log']
        standard_columns.extend(cols_retention)
        tmp_user_activity = tmp_user_activity.select(*standard_columns)

        # Append cohort's user activity data to final list
        output_range.append(tmp_user_activity)

    # Reduce all dataframe to overwrite original
    user_activity = reduce(DataFrame.unionAll, output_range)
    user_activity = user_activity.orderBy(['first_log'])

    return user_activity