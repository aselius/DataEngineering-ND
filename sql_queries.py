# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplay (songplay_id serial PRIMARY KEY, start_time integer REFERENCES time (start_time),
user_id integer REFERENCES users (user_id), level varchar REFERENCES users (level),
song_id integer REFERENCES songs (song_id), artist_id integer REFERENCES artists (artist_id), session_id serial,
location varchar, user_agent varchar);
""")

user_table_create = ("""CREATE TABLE users (user_id serial PRIMARY KEY, first_name varchar, last_name varchar,
gender varchar, level varchar);
""")

song_table_create = ("""CREATE TABLE songs (song_id serial PRIMARY KEY, title varchar,
artist_id integer REFERECES artists (artist_id), year integer, duration float);
""")

artist_table_create = ("""CREATE TABLE artists (artist_id serial PRIMARY KEY, name varchar, location varchar,
latitude integer, longitude integer);
""")

time_table_create = ("""CREATE TABLE time (start_time integer, hour integer, day integer, week integer, month integer,
year integer, weekday varchar);
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]