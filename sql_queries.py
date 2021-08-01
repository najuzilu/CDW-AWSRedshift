import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

SCHEMA_NAME = config.get("AWS", "SCHEMA_NAME")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays cascade;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """CREATE TABLE IF NOT EXISTS staging_events \
    (artist varchar, auth varchar, firstName varchar, gender char, itemInSession int,\
    lastName varchar, length float, level varchar, location varchar, method varchar, \
    page varchar,registration timestamp, sessionId int, song varchar, status int, \
    ts timestamp, userAgent varchar, userId int);"""

staging_songs_table_create = """CREATE TABLE IF NOT EXISTS staging_songs \
    (num_songs int, artist_id varchar, artist_latitude float, \
    artist_longitude float, artist_location varchar, artist_name varchar,\
    song_id varchar, title varchar, duration float, year int);"""

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays \
    (songplay_id int IDENTITY(0,1) PRIMARY KEY distkey, \
    start_time timestamp NOT NULL, user_id int NOT NULL sortkey, \
    level varchar, song_id varchar NOT NULL, artist_id varchar NOT NULL, \
    session_id int NOT NULL, location varchar, user_agent varchar);"""

user_table_create = """CREATE TABLE IF NOT EXISTS users \
    (user_id int PRIMARY KEY sortkey, first_name varchar, last_name varchar, \
    gender char, level varchar);"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs \
    (song_id varchar PRIMARY KEY sortkey, title varchar NOT NULL, artist_id varchar,\
    year int, duration float) diststyle all;"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists \
    (artist_id varchar PRIMARY KEY sortkey, name varchar NOT NULL, location varchar, \
    latitude float, longitude float) diststyle all;"""

time_table_create = """CREATE TABLE IF NOT EXISTS time \
    (start_time timestamp PRIMARY KEY sortkey, hour int, day int, week int, \
    month int, year int, weekday int);"""

# STAGING TABLES

staging_events_copy = """
copy {}.staging_events from '{}'
iam_role '{}'
json '{}'
timeformat 'epochmillisecs'
region 'us-west-2';
"""

staging_songs_copy = """
copy {}.staging_songs from '{}'
iam_role '{}'
json 'auto'
region 'us-west-2';
"""

# FINAL TABLES

songplay_table_insert = f"""INSERT INTO {SCHEMA_NAME}.songplays \
    (start_time, user_id, level, song_id, artist_id, session_id, \
    location, user_agent) SELECT \
    staging_events.ts as start_time, \
    staging_events.userId as user_id, \
    staging_events.level as level, \
    staging_songs.song_id as song_id, \
    staging_songs.artist_id as artist_id, \
    staging_events.sessionId as session_id, \
    staging_events.location as location, \
    staging_events.userAgent as user_agent \
    FROM {SCHEMA_NAME}.staging_events \
    JOIN {SCHEMA_NAME}.staging_songs \
    ON (staging_events.artist=staging_songs.artist_name AND \
        staging_events.song=staging_songs.title)
    WHERE staging_events.page='NextSong';"""

user_table_insert = f"""INSERT INTO {SCHEMA_NAME}.users \
    (user_id, first_name, last_name, gender, level) SELECT \
    DISTINCT(staging_events.userId) as user_id, \
    staging_events.firstName as first_name, \
    staging_events.lastName as last_name, \
    staging_events.gender as gender, \
    staging_events.level as level \
    FROM {SCHEMA_NAME}.staging_events
    WHERE user_id IS NOT NULL AND staging_events.page='NextSong';"""


song_table_insert = f"""INSERT INTO {SCHEMA_NAME}.songs \
    (song_id, title, artist_id, year, duration) SELECT \
    DISTINCT(staging_songs.song_id) as song_id, \
    staging_songs.title as title, \
    staging_songs.artist_id as artist_id, \
    staging_songs.year as year, \
    staging_songs.duration as duration \
    FROM {SCHEMA_NAME}.staging_songs;"""

artist_table_insert = f"""INSERT INTO {SCHEMA_NAME}.artists \
    (artist_id, name, location, latitude, longitude) SELECT \
    DISTINCT(staging_songs.artist_id) as artist_id, \
    staging_songs.artist_name as name, \
    staging_songs.artist_location as location, \
    staging_songs.artist_latitude as latitude, \
    staging_songs.artist_longitude as longitude \
    FROM {SCHEMA_NAME}.staging_songs;"""

time_table_insert = f"""INSERT INTO {SCHEMA_NAME}.time \
    (start_time, hour, day, week, month, year, weekday) SELECT \
    DISTINCT(songplays.start_time) as start_time, \
    EXTRACT(hour from songplays.start_time) as hour, \
    EXTRACT(day from songplays.start_time) as day, \
    EXTRACT(week from songplays.start_time) as week, \
    EXTRACT(month from songplays.start_time) as month, \
    EXTRACT(year from songplays.start_time) as year, \
    EXTRACT(dow from songplays.start_time) as weekday \
    FROM {SCHEMA_NAME}.songplays;"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    staging_events_table_drop,
    staging_songs_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
