import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
import calendar


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function loads song_data from S3 and 
                 processes it by extracting the songs and artist tables
                 and then again loaded back to S3
        
    Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files
            output_data : S3 bucket were dimensional tables in parquet format 
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table_query = """
                        SELECT song_id, title, artist_id, year, duration
                        FROM song_data_table
                        WHERE song_id IS NOT NULL
                        """
    songs_table = spark.sql(songs_table_query)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")

    
    # extract columns to create artists table
    artists_table_query = """
                        SELECT DISTINCT artist_id, 
                               artist_name,
                               artist_location,
                               artist_latitude,
                               artist_longitude
                        FROM song_data_table
                        WHERE artist_id IS NOT NULL
                        """
    
    artists_table = spark.sql(artists_table_query)
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_date + "/artists/artists.parquet", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Description: This function loads log_data from S3 and processes it 
                 by extracting the songs and artist tables 
                and then again loaded back to S3
        
    Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files
            output_data : S3 bucket were dimensional tables in parquet format
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table
    users_query = """
                 SELECT DISTINCT userId user_id, 
                                 firstName first_name, 
                                 lastName last_name,
                                 gender,
                                 level
                 FROM log_data_table 
                 WHERE userId IS NOT NULL
                  """
    users_table = spark.sql(users_query).dropDuplicates(['userId', 'level'])
    
    # write users table to parquet files
    users_table.write.parquet(path = output_data + "/users/users.parquet", mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    get_week = udf(lambda x: calendar.day_name[x.weekday()])
    get_weekday = udf(lambda x: datetime.isocalendar(x)[1])
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x : x.day)
    get_year = udf(lambda x: x.year)
    get_month = udf(lambda x: x.month)
    
    df = df.withColumn('start_time', get_datetime(df.ts))
    df = df.withColumn('hour', get_hour(df.start_time))
    df = df.withColumn('day', get_day(df.start_time))
    df = df.withColumn('week', get_week(df.start_time))
    df = df.withColumn('month', get_month(df.start_time))
    df = df.withColumn('year', get_year(df.start_time))
    df = df.withColumn('weekday', get_weekday(df.start_time))
    
    # extract columns to create time table
    time_table = df['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_table = time_table.drop_duplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.title == df.song) & (song_df.artist_name == df.artist))\
                        .distinct().select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')
                        .withColumn('songplay_id', monotonically_increasing_id()) 
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + "songplays.parquet", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-project-results"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
