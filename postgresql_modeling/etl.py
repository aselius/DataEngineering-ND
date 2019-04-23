import collections
import datetime
import os
import glob
import json

import psycopg2

from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    with open(filepath, 'r') as f:
        data = json.load(f)
    data = collections.OrderedDict(sorted(data.items()))

    song_keys = ['song_id', 'title', 'artist_id', 'year', 'duration']

    # TODO: Change the table create query so that the column names are alphabetical to avoid doing this resort
    # insert artist record
    artist_data = [data['artist_id'], data['artist_name'], data['artist_location'], data['artist_latitude'],
                   data['artist_longitude']]
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = [data['song_id'], data['title'], data['artist_id'], data['year'], data['duration']]
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    # open log file
    data = []
    with open(filepath, 'r') as f:
        for line in f:
            data.append(json.loads(line))

    # filter by NextSong action
    data = [entry for entry in data if entry['page'] == 'NextSong']

    # convert timestamp column to datetime
    for idx, entry in enumerate(data):
        data[idx]['datetime'] = (datetime.datetime.fromtimestamp(entry['ts'] / 1000))

    ts = []
    # convert datetime objects into time format we want
    for idx, row in enumerate(data):
        dt = row['datetime']
        ts.append([dt.timestamp(), dt.hour, dt.day, dt.isocalendar()[1], dt.month, dt.year,
                   datetime.datetime.weekday(dt)])

    for row in ts:
        cur.execute(time_table_insert, list(row))

    # load user table
    users = []
    for entry in data:
        users.append([entry['userId'], entry['firstName'], entry['lastName'], entry['gender'], entry['level']])

    # insert user records
    for row in users:
        cur.execute(user_table_insert, row)

    # insert songplay records
    for row in data:

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row['song'], row['artist'], row['length']))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row['datetime'].timestamp(), row['userId'], row['level'], songid, artistid, row['sessionId'],
                         row['location'], row['userAgent']]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect(
        host='localhost',
        port=54320,
        dbname='sparkifydb',
        user='postgres'
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()