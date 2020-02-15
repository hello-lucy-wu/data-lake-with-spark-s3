# data-lake-with-spark-s3

This project is to build a data lake based on [song data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/song_data/?region=us-west-2&tab=overview) and [log data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/log_data/?region=us-west-2&tab=overview) residing in S3. <br /><br />
The script, etl.py, reads data from S3 and transforms them to create five different tables(Users, Artists, Songs, Time and Songplays). Each of the tables are written to parquet files in a separate analytics directory on S3.  <br /><br /> Songs table files are partitioned by year and artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.
The data structures of tables are the same as the one in my previous project  [data-modeling-with-postgres](https://github.com/hello-lucy-wu/data-modeling-with-postgres#Data). 
### Table of Contents
* [Tables](#Tables)
* [Steps to run scripts](#Steps)

### Tables
* There are four dimension tables and one fact tables.
    - Fact Table \
        songplays - records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    - Dimension Tables \
        users - users in the app
        user_id, first_name, last_name, gender, level

        songs - songs in music database
        song_id, title, artist_id, year, duration

        artists - artists in music database
        artist_id, name, location, latitude, longitude

        time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday


### Steps 
* replace AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in dl.cfg with your own AWS credentials
* run `etl.py`
