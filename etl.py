import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np

def process_song_file(cur, filepath):
    """
    Process json song files with absolute path(filepath) and insert the target values into tables (songs, artists).
    Params:
        cur: cursor for sparkifydb connection.
        filepath: the json file for song in absolute path to be processed.
    Return:
        None. the function will process the data and then insert data into songs and artists table.
    """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process json log files with absolute path(filepath) and insert the target values into the time, users and songplays table.
    Params:
        cur: cursor for sparkifydb connection.
        filepath: the json file for log in absolute path to be processed.
    Return:
        None. the function will process the data and then insert data into the time, users and songplays table.
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts']/1000, unit = 's')
    
    # insert time data records
    time_data = np.transpose(
                np.array(
                    [df['ts'].values, t.dt.hour.values, \
                  t.dt.day.values, t.dt.week.values, \
                  t.dt.month.values, t.dt.year.values, \
                  t.dt.weekday.values]))
    column_labels = ('timestamp','hour','day','week_of_year','month','year','weekday')

    time_df = pd.DataFrame(data = time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Get all the json files for the given filepath, and process the data and insert data into tables by invoking the func.
    Params:
        cur: cursor for sparkifydb connection.
        conn: connection for spkarkifydb connection.
        filepath: the json file path where the json files are stored.
        func: the function invoked to process the specific data and insert the processed data into table.
    Return:
        None. the function will insert the processed data into tables according to the given func.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()