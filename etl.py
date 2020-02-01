import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date

config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def generate_log_schema():    
    log_schema = R([
        Fld("num_songs",Int()),
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_name",Str()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("duration",Dbl()),
        Fld("year",Int())
    ])
    return log_schema

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + '/song-data'
    
    log_schema = generate_log_schema()

    # read song data file
    df = spark.read.json(song_data, schema=log_schema)
    df.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT year, artist_id, song_id, duration
        FROM song_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.partitionBy([col("year"), col("artist_id")]).write.parquet("/location")

    # # extract columns to create artists table
    # artists_table = 
    
    # # write artists table to parquet files
    # artists_table


# def process_log_data(spark, input_data, output_data):
#     # get filepath to log data file
#     log_data = input_data + '/log-data/*.json'

#     # read log data file
#     df = spark.read.json(log_data)
    
#     # filter by actions for song plays
#     df = 

#     # extract columns for users table    
#     artists_table = 
    
#     # write users table to parquet files
#     artists_table

#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
#     # extract columns to create time table
#     time_table = 
    
#     # write time table to parquet files partitioned by year and month
#     time_table

#     # read in song data to use for songplays table
#     song_df = 

#     # extract columns from joined song and log datasets to create songplays table 
#     songplays_table = 

#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    input_data = "./data"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
