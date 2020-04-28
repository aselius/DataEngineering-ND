import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
    id INTEGER IDENTITY(0,1),
    artist VARCHAR(MAX),
    auth VARCHAR(15),
    firstName VARCHAR(255),
    gender VARCHAR(5),
    itemInSession INTEGER,
    lastName VARCHAR(255),
    length DOUBLE PRECISION,
    level VARCHAR(50),
    location VARCHAR(MAX),
    method VARCHAR(6),
    page VARCHAR(MAX),
    registration DOUBLE PRECISION,
    sessionId DOUBLE PRECISION,
    song VARCHAR(MAX),
    status numeric(3,0),
    ts TIMESTAMP,
    userAgent VARCHAR(MAX),
    userId VARCHAR(50), 
    PRIMARY KEY (id)
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(MAX),
    artist_name VARCHAR(MAX),
    song_id VARCHAR(100),
    title VARCHAR(MAX),
    duration DOUBLE PRECISION,
    year numeric(4,0),
    PRIMARY KEY (song_id)
);
""")

songplay_table_create = ("""
CREATE TABLE songplay 
(
    songplay_id INTEGER NOT NULL,
    start_time TIMESTAMP,
    user_id VARCHAR(50) REFERENCES users(user_id),
    level VARCHAR(10),
    song_id VARCHAR(18) REFERENCES songs(song_id),
    artist_id VARCHAR(18) REFERENCES artists(artist_id),
    location VARCHAR(MAX),
    user_agent VARCHAR(MAX),
    PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR NOT NULL,
    first_name VARCHAR(25),
    last_name VARCHAR(25),
    gender VARCHAR(5),
    level VARCHAR(10),
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR(18) NOT NULL,
    title VARCHAR(MAX),
    artist_id VARCHAR(18),
    year numeric(4,0),
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR(18) NOT NULL,
    name VARCHAR(MAX),
    location VARCHAR(MAX),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP NOT NULL,
    hour numeric(2,0) NOT NULL,
    day numeric(2,0) NOT NULL,
    week numeric(2,0) NOT NULL,
    month numeric(2,0) NOT NULL,
    year numeric(4,0) NOT NULL,
    weekday VARCHAR(9) NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
JSON {}
TIMEFORMAT as 'epochmillisecs';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (songplay_id, start_time, user_id, level, song_id, artist_id, location, user_agent)
    SELECT DISTINCT se.id, se.ts, se.userId, se.level, ss.song_id, ss.artist_id, se.location, se.userAgent
        FROM staging_events se JOIN staging_songs ss ON (se.artist = ss.artist_name and se.song = ss.title)
            WHERE se.id IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts,
        extract(hour from ts) as hour,
        extract(day from ts) as day,
        extract(week from ts) as week,
        extract(month from ts) as month,
        extract(year from ts) as year,
        extract(weekday from ts) as weekday
        FROM staging_events WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
