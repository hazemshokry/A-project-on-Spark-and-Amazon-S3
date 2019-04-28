import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr('song_id', 'title', 'artist_id',
                                'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id'). \
    parquet(output_data+'songs')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name',
                                  'artist_location', 
                                  'artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+"log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.selectExpr('userId', 'firstName', 'lastName',
                                'gender', 'level')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(to_timestamp)
    df = df.withColumn('new_ts', get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    df = df.withColumn('new_date', to_date("new_ts", "timestamp"))
    
    # extract columns to create time table
    time_table = df.select('new_ts', hour('new_ts').alias('hour'),
                             dayofmonth('new_date').alias('day'),
                             weekofyear('new_date').alias('week'),
                            month('new_date').alias('month'),
                            year('new_date').alias('year'),
                            date_format('new_ts', 'EEEE').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month'). \
    mode('overwrite').parquet(output_data+'times')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs')

    # extract columns from joined song and log datasets to create songplays table
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')
    songplays_table = spark.sql(""" 
    SELECT 
        row_number() over (partition by new_ts order by new_ts) as id,
        new_ts,
        userId,
        level,
        song_id,
        artist_id,
        sessionId,
        location,
        userAgent
        FROM songs s JOIN events e
        ON s.title = e.song
        AND s.duration = e.length
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.select(col("*"), year('new_ts').alias('year'), 
                           month('new_ts').alias('month')) \
    .write \
    .partitionBy('year', 'month') \
    .mode('overwrite') \
    .parquet(output_data+'song_plays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "" #Put your bucket here
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
