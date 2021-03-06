# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(songplay_id SERIAL PRIMARY KEY,
 start_time BIGINT NOT NULL,
 user_id int NOT NULL,
 level text,
 song_id text,
 artist_id text,
 session_id int,
 location text,
 user_agent text)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id int NOT NULL PRIMARY KEY,
first_name text NOT NULL,
last_name text NOT NULL,
gender text,
level text)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id text NOT NULL PRIMARY KEY,
 title text NOT NULL,
 artist_id text NOT NULL,
 year int,
 duration float CHECK (duration>=0))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id text NOT NULL PRIMARY KEY,
 name text NOT NULL,
 location text,
 latitude float,
 longitude float)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time bigint NOT NULL PRIMARY KEY,
 hour int,
 day int,
 week_of_year int,
 month int,
 year int,
 weekday int)
""")

# INSERT RECORDS
songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week_of_year, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

# FIND SONGS
song_select = ("""
SELECT s.song_id, a.artist_id 
FROM songs s 
JOIN artists a ON s.artist_id = a.artist_id 
WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]