import configparser


# https://vogbmh3oio.prod.udacity-student-workspaces.com/edit/sql_queries.py
## CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_data"
user_table_drop = "DROP TABLE IF EXISTS user_data"
song_table_drop = "DROP TABLE IF EXISTS song_data"
artist_table_drop = "DROP TABLE IF EXISTS artist_data"
time_table_drop = "DROP TABLE IF EXISTS time_data"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
    artist              VARCHAR(250),
    auth                VARCHAR(20),
    first_name          TEXT,
    gender              TEXT,
    item_in_session     INT,
    last_name           TEXT,
    length              DECIMAL,
    level               TEXT,
    location            VARCHAR(50),
    method              TEXT,
    page                VARCHAR(20),
    registration        BIGINT,
    session_id          INT            NOT NULL,
    song_title          VARCHAR(250),
    status              INT,
    ts                  BIGINT         NOT NULL,
    user_agent          VARCHAR(150),
    user_id             INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
    artist_id           VARCHAR(25)    NOT NULL    PRIMARY KEY,
    artist_latitude     DECIMAL(5),
    artist_location     VARCHAR(300),
    artist_longitude    DECIMAL(5),
    artist_name         VARCHAR(300),
    duration            DECIMAL(5),
    num_songs           INT,
    song_id             VARCHAR(30)    NOT NULL,
    title               VARCHAR(200),
    year                INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplay_data
(
    songplay_id        INT              IDENTITY(0,1)    PRIMARY KEY,
    start_time         TIMESTAMP        NOT NULL,
    user_id            INT              NOT NULL,
    level              TEXT,
    song_id            VARCHAR(30),
    artist_id          VARCHAR(25)      NOT NULL,
    session_id         INT,
    location           VARCHAR(200),
    user_agent         VARCHAR(150)
);
""")

user_table_create = ("""
CREATE TABLE user_data
(
    user_id            INT    PRIMARY KEY,
    first_name         TEXT,
    last_name          TEXT,
    gender             TEXT,
    level              TEXT     
);
""")

song_table_create = ("""
CREATE TABLE song_data
(
   song_id             VARCHAR(30)      PRIMARY KEY,
   title               VARCHAR(200)     NOT NULL,
   artist_id           VARCHAR(25)      NOT NULL,
   year                INT,
   duration            DECIMAL          
); 
""")

artist_table_create = ("""
CREATE TABLE artist_data
(
   artist_id           VARCHAR(25)     PRIMARY KEY,
   name                VARCHAR(100),
   location            VARCHAR(200),
   latitude            DECIMAL,
   longitude           DECIMAL
);
""")

time_table_create = ("""
CREATE TABLE time_data
(
    start_time         TIMESTAMP     PRIMARY KEY,
    hour               INT           NOT NULL,
    day                TEXT          NOT NULL,
    week               TEXT          NOT NULL,
    month              TEXT          NOT NULL,
    year               INT           NOT NULL,
    weekday            TEXT          NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT as JSON {}
REGION 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs 
FROM {} 
CREDENTIALS 'aws_iam_role={}'
FORMAT as JSON 'auto'
REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_data (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + (e.ts / 1000) * interval '1 second' as start_time,
        e.user_id, 
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent  
    FROM staging_events e, staging_songs s
    WHERE e.page = 'NextSong'
        AND e.song_title = s.title
        AND e.artist = s.artist_name 
        AND e.length = s.duration
""")

user_table_insert = ("""
INSERT INTO user_data (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM
        staging_events
    WHERE
        page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO song_data (song_id, title, artist_id, year, duration)
SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artist_data select distinct (artist_id)
        artist_id,
        artist_name,
        artist_location, 
        artist_latitude,
        artist_longitude
  FROM  staging_songs
  WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time_data(start_time, hour, day, week, month, year, weekday)
        SELECT 
        start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(dayofweek from start_time)
  FROM  songplay_data
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
