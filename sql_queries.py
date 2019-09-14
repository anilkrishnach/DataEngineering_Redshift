import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS STAGING_EVENTS(
ARTIST VARCHAR SORTKEY,
AUTH VARCHAR,
FIRSTNAME VARCHAR,
GENDER CHAR(1),
ITEMINSESSION INT,
LASTNAME VARCHAR,
LENGTH DECIMAL,
LEVEL VARCHAR,
LOCATION VARCHAR,
METHOD VARCHAR,
PAGE VARCHAR,
REGISTRATION DECIMAL,
SESSIONID INT,
SONG VARCHAR DISTKEY,
STATUS INT,
TS BIGINT,
USERAGENT VARCHAR,
USERID INT
)DISTSTYLE KEY
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS STAGING_SONGS(
ARTIST_ID VARCHAR,
ARTIST_LATITUDE DECIMAL,
ARTIST_LOCATION VARCHAR,
ARTIST_LONGITUDE DECIMAL,
ARTIST_NAME VARCHAR SORTKEY,
DURATION DECIMAL,
NUM_SONGS INT,
SONG_ID VARCHAR,
TITLE VARCHAR DISTKEY,
YEAR INT
)DISTSTYLE KEY
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGPLAYS(
SONGPLAY_ID BIGINT IDENTITY(0,1) PRIMARY KEY,
START_TIME TIMESTAMP NOT NULL,
USER_ID INT NOT NULL,
LEVEL VARCHAR,
SONG_ID VARCHAR,
ARTIST_ID VARCHAR,
SESSION_ID INT,
LOCATION VARCHAR,
USER_AGENT VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS USERS(
USER_ID INT PRIMARY KEY,
FIRST_NAME VARCHAR,
LAST_NAME VARCHAR,
GENDER CHAR(1),
LEVEL VARCHAR
)
""")


artist_table_create = ("""
CREATE TABLE  IF NOT EXISTS ARTISTS(
ARTIST_ID VARCHAR PRIMARY KEY,
NAME VARCHAR,
LOCATION VARCHAR,
LATITUDE DECIMAL,
LONGITUDE DECIMAL
)
""")


song_table_create = ("""
CREATE TABLE  IF NOT EXISTS SONGS(
SONG_ID VARCHAR PRIMARY KEY,
TITLE VARCHAR,
ARTIST_ID VARCHAR ,
YEAR INT,
DURATION DECIMAL NOT NULL
)
""")

time_table_create = ("""
CREATE TABLE  IF NOT EXISTS TIME(
START_TIME TIMESTAMP PRIMARY KEY,
HOURS INT,
DAY INT,
WEEK INT,
MONTH INT,
YEAR INT,
WEEKDAY VARCHAR
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY STAGING_EVENTS
FROM '{}'
IAM_ROLE {}
REGION '{}'
JSON '{}'
""").format('s3://udacity-dend/log_data', config.get('IAM_ROLE', 'ARN'), 'us-west-2', 's3://udacity-dend/log_json_path.json')

staging_songs_copy = ("""
COPY STAGING_SONGS
FROM '{}'
IAM_ROLE {}
REGION '{}'
JSON 'auto'
""").format('s3://udacity-dend/song_data', config.get('IAM_ROLE', 'ARN'), 'us-west-2')

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO SONGPLAYS (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
(
SELECT TIMESTAMP 'epoch' + E.TS * interval '0.001 second', E.USERID, E.LEVEL, S.SONG_ID, S.ARTIST_ID, E.SESSIONID, E.LOCATION, E.USERAGENT
FROM STAGING_EVENTS E
JOIN STAGING_SONGS S ON E.SONG = S.TITLE AND E.ARTIST = S.ARTIST_NAME
)
""")

user_table_insert = ("""
INSERT INTO USERS 
(
SELECT E.USERID, MAX(E.FIRSTNAME), MAX(E.LASTNAME), MAX(E.GENDER), MAX(E.LEVEL)
FROM STAGING_EVENTS E
WHERE E.USERID IS NOT NULL
GROUP BY E.USERID
)
""")

song_table_insert = ("""
INSERT INTO SONGS 
(
SELECT S.SONG_ID, S.TITLE, S.ARTIST_ID, S.YEAR, S.DURATION
FROM STAGING_SONGS S
)
""")

artist_table_insert = ("""
INSERT INTO ARTISTS 
(
SELECT S.ARTIST_ID, S.ARTIST_NAME, S. ARTIST_LOCATION, S.YEAR, S.DURATION
FROM STAGING_SONGS S
)
""")


time_table_insert = ("""
INSERT INTO TIME 
(
SELECT E.START_TIME AS TS, EXTRACT(H FROM TS), EXTRACT(D FROM TS), EXTRACT(W FROM TS), EXTRACT(MON FROM TS), EXTRACT(Y FROM TS), EXTRACT(DW FROM TS)
FROM SONGPLAYS E
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


