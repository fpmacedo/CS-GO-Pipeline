import os
import configparser

# pylint: disable=import-error
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T
from pyspark.sql.functions import monotonically_increasing_id, col

def create_spark_session():
    
    """
    Create the spark session with the passed configs.
    """
    
    spark = SparkSession \
        .builder \
        .appName("CS-GO-Pipeline")\
        .getOrCreate()

    return spark

def process_match_results(spark, input_data, output_data):

    """
    Perform ETL on match_results to create the time, team dimensional tables and match_results fact table: 
    - Remove corrupt records and null values. 
    - Extract time data and insert in the time table.
    - Extract team data and insert in the team table.
    - Extract the match result data and insert in the match_results table.
      
    Parameters:
    - spark: spark session
    - input_data : path to input files
    - output_data : path to output files
    """

    # get filepath to match results data file
    match_results_data = os.path.join(input_data,"match_results/match_results.json")

    #reading json files
    df_match_results = spark.read.json(match_results_data)

    #remove corrupt records and null lines
    df_match_results.printSchema()
    df_match_results = df_match_results.drop("_corrupt_record")
    df_match_results = df_match_results.dropna()

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType())
    # create datetime column from original timestamp column
    #get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    get_hour = udf(lambda x: x.hour, T.IntegerType()) 
    get_day = udf(lambda x: x.day, T.IntegerType()) 
    get_week = udf(lambda x: x.isocalendar()[1], T.IntegerType()) 
    get_month = udf(lambda x: x.month, T.IntegerType()) 
    get_year = udf(lambda x: x.year, T.IntegerType()) 
    get_weekday = udf(lambda x: x.weekday(), T.IntegerType())

    df_match_results = df_match_results.withColumn("timestamp", get_timestamp(df_match_results.data_unix))
    df_match_results = df_match_results.withColumn("hour", get_hour(df_match_results.timestamp))
    df_match_results = df_match_results.withColumn("day", get_day(df_match_results.timestamp))
    df_match_results = df_match_results.withColumn("week", get_week(df_match_results.timestamp))
    df_match_results = df_match_results.withColumn("month", get_month(df_match_results.timestamp))
    df_match_results = df_match_results.withColumn("year", get_year(df_match_results.timestamp))
    df_match_results = df_match_results.withColumn("weekday", get_weekday(df_match_results.timestamp))

    #CREATE TIME TABLE

    # extract columns to create time table
    time_columns = ['data_unix','timestamp','hour', 'day', 'week', 'month', 'year', 'weekday']
    time_table = df_match_results[time_columns].distinct() 

    # write time table to parquet files
    time_table.write.parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print("--- time.parquet completed ---")

    #CREATE TEAM TABLE

    #create dataframes with team_1 and team_2 data
    team_1_df = df_match_results['team_1_id', 'team_1_link', 'team_1_name']
    team_2_df = df_match_results['team_2_id', 'team_2_link', 'team_2_name']

    #append the two team dataframes
    all_teams_df = team_1_df.unionAll(team_2_df)

    #rename the columns of the dataframe
    new_cols = ['team_id', 'team_link', 'team_name']
    for i in range(len(all_teams_df.columns)):
        col_names = all_teams_df.columns
        all_teams_df = all_teams_df.withColumnRenamed(col_names[i], new_cols[i])

    #select just the distinct values
    team_table = all_teams_df.distinct()

    # write team table to parquet files
    team_table.write.parquet(os.path.join(output_data, 'team.parquet'), 'overwrite')
    print("--- team.parquet completed ---")

    #CREATE MATCHES TABLE

    # extract columns to create matches table
    matches_columns = ['match_id', 'data_unix', 'team_1_id', 'team_1_score', 'team_2_id', 'team_2_score', 'map', 'event_name', 'month', 'year', 'offset']

    #create the matches table and select the distinct values
    matches_table = df_match_results[matches_columns].distinct()
    
    # write matches table to parquet files
    matches_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'matches.parquet'), 'overwrite')
    print("--- matches.parquet completed ---")



def process_match_players(spark, input_data, output_data):
  
    """
    Perform ETL on match_players to create the match_players dimensional tables: 
    - Remove corrupt records and null values. 
    - Extract the players scores data and insert in the match_players table.
      
    Parameters:
    - spark: spark session
    - input_data : path to input files
    - output_data : path to output files
    """
    # get filepath to match players data file
    match_players_data = os.path.join(input_data,"match_players/match_players.json")

    #reading json files
    df_match_players = spark.read.json(match_players_data)

    #remove corrupt records and null lines
    df_match_players = df_match_players.drop("_corrupt_record")
    df_match_players = df_match_players.dropna()

    #CREATE MATCH_PLAYERS TABLE

    # extract columns to create match players table
    match_players_columns = ['match_id', 'player_id', 'match_link', 'team_name', 'kills', 'assists', 'deaths', 'hs', 'flash_assists', 'kdratio', 'adr', 'fkdiff', 'rating']
    
    #create the match players table and select the distinct values
    match_players = df_match_players[match_players_columns].distinct()

    # write match players table to parquet files
    match_players.write.partitionBy('match_id').parquet(os.path.join(output_data, 'match_players.parquet'), 'overwrite')
    print("--- match_players.parquet completed ---")

def process_player_stats(spark, input_data, output_data):
  
    """
    Perform ETL on player_stats to create the player_stats dimensional tables: 
    - Remove corrupt records and null values. 
    - Extract the player stats data and insert in the player_stats table.
      
    Parameters:
    - spark: spark session
    - input_data : path to input files
    - output_data : path to output files
    """

    # get filepath to players stats data file
    players_stats_data = os.path.join(input_data,"players_stats/players_stats.json")

    #reading json files
    df_players_stats = spark.read.json(players_stats_data)
    df_players_stats = df_players_stats.drop("_corrupt_record")
    df_players_stats = df_players_stats.dropna()
    
    #create the players stats and select the distinct values
    players_stats = df_players_stats.distinct()

    # write players stats table to parquet files
    players_stats.write.parquet(os.path.join(output_data, 'players_stats.parquet'), 'overwrite')
    print("--- players_stats.parquet completed ---")

def data_quality(spark, input_data, output_data):

    match_results_data = os.path.join(input_data,"players_stats.parquet")
    players_stats_parquet=spark.read.parquet(match_results_data)

    players_stats_parquet = players_stats_parquet.filter(players_stats_parquet.player_id == 0)
    
    if players_stats_parquet.count() == 0:
      #raise ValueError(f"Data quality check failed player stats is empty.")
      print("Data quality check failed player stats is empty.")

def main():
    
    """
    Build ETL Pipeline for CS GO PIPELINE:
    
    Call the function to create a spark session;
    Instantiate the input and output paths;
    Call the process functions.
    """
    
    spark = create_spark_session()
    input_data = "s3n://fpmacedo/"
    output_data = "s3n://fpmacedo/"
    
    process_match_results(spark, input_data, output_data)    
    process_match_players(spark, input_data, output_data)
    process_player_stats(spark, input_data, output_data)
    data_quality(spark, input_data, output_data)

if __name__ == "__main__":
    main()