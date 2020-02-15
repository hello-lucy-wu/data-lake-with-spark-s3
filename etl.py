import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, to_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long, TimestampType as Ts

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.5")\
        .enableHiveSupport()\
        .getOrCreate()

    return spark


def generate_song_data_schema():    
    song_data_schema = R([
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
    return song_data_schema


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')

    song_data_schema = generate_song_data_schema()

    # read song data file
    df = spark.read.json(song_data, schema=song_data_schema)

    df.createOrReplaceTempView('song_data')

    # extract columns to create songs dataframe
    songs_df = spark.sql("""
        SELECT 
            song_id, 
            title, 
            artist_id,
            year,
            duration
        FROM song_data
        WHERE year IS NOT NULL and artist_id IS NOT NULL
    """)
    
    # write songs dataframe to parquet files partitioned by year and artist id
    songs_df.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists dataframe
    artists_df = spark.sql("""
        SELECT
            artist_id, 
            artist_name as name, 
            artist_location as location,
            artist_longitude as longitude, 
            artist_latitude as latitude
        FROM song_data
        WHERE artist_id IS NOT NULL
    """)
    
    # write artists dataframe to parquet files
    artists_df.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x)//1000, Int())
    df = df.withColumn('timestamp', get_timestamp(df.ts))

   
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), Ts())
    df = df.withColumn('datetime', get_datetime(df.timestamp))
    

    # create dataframe with correct data type
    df = df.withColumn('itemInSession', df["itemInSession"].cast(Int()))
    df = df.withColumn('registration', df["registration"].cast(Long()))
    df = df.withColumn('status', df["status"].cast(Int()))
    df = df.withColumn('userId', df["userId"].cast(Int()))

    df.createOrReplaceTempView("log_data")
    
    # extract columns for users dataframe    
    users_df = spark.sql("""
        SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level
        FROM log_data
        where userId IS NOT NULL
    """)

    # write users dataframe to parquet files
    users_df.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # extract columns to create time dataframe
    time_df = spark.sql("""
        SELECT 
            timestamp as start_time,
            hour(datetime) as hour,
            dayofmonth(datetime) as day, 
            weekofyear(datetime) as week, 
            month(datetime) as month, 
            year(datetime) as year, 
            dayofweek(datetime) as weekday
        FROM log_data
    """)
    
    # write time dataframe to parquet files partitioned by year and month
    time_df.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read in song data to use for songplays dataframe
    songs_df = spark.read.load(output_data + "/songs")

    songplays_df = df.join(songs_df, df.song == songs_df.title, 'left')

    songplays_df.createOrReplaceTempView("songplays")

    songplays_df = spark.sql("""
        SELECT
             timestamp as start_time,
             userId as user_id,
             level,
             song_id,
             artist_id,
             sessionId as session_id,
             location,
             userAgent as user_agent,
             month(datetime) as month,
             year(datetime) as year
        FROM songplays
    """)
    songplays_df = songplays_df.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays dataframe to parquet files partitioned by year and month
    songplays_df.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://data-lake-with-spark-s3/'

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
