import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates a spark session for context
    
    :returns spark: spark session context
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extracts the song and artist data from song files and writes parquet files to s3
    """
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'duration', 'artist_id', 'year')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = df.write.partitionBy(col('year'), col('artist')).parquet(f"{output_data}songs", mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    output_path = os.path.join(output_data, 'artists')
    artists_table.write.parquet(output_path)


def process_log_data(spark, input_data, output_data):
    """
    Extracts the time, user and songplay data from log files and writes to s3
    """
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page.isin("NextSong"))

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    
    # write users table to parquet files
    user_output_path = os.path.join(output_data, 'users')
    users_table.write.parquet(user_output_path)

    # create timestamp column from original timestamp column
    date_format = '%Y-%m-%d %H:%M:%S'
    get_timestamp = udf(lambda x:datetime.fromtimestamp(x/1000).strftime(date_format))
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # extract columns to create time table
    weekday = udf(lambda x:datetime.strptime(x, date_format).strftime('%a'))
    time_table = df.select(col('start_time'),
                           hour(col('start_time')).alias('hour'),
                           dayofmonth(col('start_time')).alias('day'), 
                           weekofyear(col('start_time')).alias('week'), 
                           month(col('start_time')).alias('month'),
                           year(col('start_time')).alias('year'),
                           weekday(col('start_time').alias('weekday')))
    
    # write time table to parquet files partitioned by year and month
    time_output_path = os.path.join(output_data, 'time')
    time_table.write.partitionBy(col('year'),col('month')).parquet(time_output_path)
                              
    # read in song data to use for songplays table
    song_data = f"{input_data}song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data).dropDuplicates()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, song_df.artist_name==df.artist).withColumn(
        'songplay_id',
        monotonically_increasing_id()).select(
        'songplay_id',
        'start_time',
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('userAgent').alias('user_agent'),
        month(col('start_time')).alias('month'),
        year(col('start_time')).alias('year'))

    # write songplays table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, 'songplay')
    songplays_table.write.partitionBy(col('year'),col('month')).parquet(output_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
