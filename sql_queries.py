import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table_drop"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagin_songs_table_drop"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table_drop"
user_table_drop = "DROP TABLE IF EXISTS user_table_drop"
song_table_drop = "DROP TABLE IF EXISTS songplay_table_drop"
artist_table_drop = "DROP TABLE IF EXISTS artist_table_drop"
time_table_drop = "DROP TABLE IF EXISTS time_table_drop"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table(artist varchar,
                                 auth varchar, firstname varchar, gender varchar, itemInSession integer,
                                 lastName varchar, length varchar, level varchar, location varchar, method varchar,
                                 page varchar, registration varchar, sessionId varchar sortkey distkey,
                                 song varchar, status varchar, ts bigint, userAgent varchar, userId integer)""")

staging_songs_table_create = ("""create table if not exists staging_songs_table(num_songs int  null, 
                                                                                        artist_id varchar   sortkey distkey, 
                                                                                        artist_latitude float  , 
                                                                                        artist_longitude float , 
                                                                                        artist_name varchar  , 
                                                                                        song_id varchar  ,
                                                                                        length varchar  , 
                                                                                        level varchar  ,
                                                                                        location varchar  , 
                                                                                        method varchar  , 
                                                                                        page varchar  , 
                                                                                        registration varchar  ,
                                                                                        title varchar  ,
                                                                                        duration decimal  , 
                                                                                        year int   )
""")

songplay_table_create = ("""create table if not exists songplays(songplay_id int identity(0,1)  not null  sortkey,
                                                                    user_id varchar not null  distkey,
                                                                    start_time  timestamp not null,
                                                                    level varchar ,
                                                                    song_id varchar not null , 
                                                                    artist_id varchar not null ,
                                                                    session_id int ,
                                                                    location varchar ,
                                                                    user_agent varchar, 
                                                                    primary key (songplay_id));""")

user_table_create = ("""create table if not exists users(user_id int identity(0,1) not null sortkey,
                                                            first_name varchar ,
                                                            last_name varchar ,
                                                            gender varchar ,
                                                            level varchar ,
                                                            primary key (user_id)) diststyle all""")

song_table_create = ("""create table if not exists songs(song_id varchar not null sortkey,
                                                            title varchar ,
                                                            artist_id varchar ,
                                                            year  int ,
                                                            duration float ,
                                                            primary key (song_id)) diststyle all""")

artist_table_create = ("""create table if not exists artists(artist_id varchar not null sortkey,
                                                                name varchar ,
                                                                location float ,
                                                                latitude float ,
                                                                longitude varchar ,
                                                                primary key (artist_id)) diststyle all""")

time_table_create = ("""create table if not exists time(start_time timestamp not null sortkey,
                                                        hour smallint ,
                                                        day smallint ,
                                                        week smallint ,
                                                        month smallint ,
                                                        year int , 
                                                        weekday smallint ,
                                                        primary key (start_time)) diststyle all""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events_table from {}
credentials 'aws_iam_role={}'
    FORMAT AS JSON {}
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
 region 'us-west-2';
 
""").format(LOG_DATA, ARN,LOG_JSONPATH)


staging_songs_copy = ("""
copy staging_songs_table from {} 
credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
 region 'us-west-2';
""").format(SONG_DATA, ARN)




# FINAL TABLES


songplay_table_insert = ("""
    INSERT INTO songplays (             start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events_table AS se
    JOIN staging_songs_table AS ss
        ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
""")



user_table_insert = ("""
    INSERT INTO users (                 user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
    SELECT  DISTINCT se.userId          AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
    FROM staging_events_table AS se
    WHERE se.page = 'NextSong';
""")


song_table_insert = ("""
    INSERT INTO songs (                 song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM staging_songs_table AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (               artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_songs_table AS ss;
""")

time_table_insert = ("""
    INSERT INTO time (                  start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events_table                   AS se
    WHERE se.page = 'NextSong';
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]