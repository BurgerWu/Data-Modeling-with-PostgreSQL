import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function process the file in filepath with postgresql cursor. It first opens the file, retrieves desired data from it and finally insert into our database
    
    Input: Cursor and filepath
    Output: No direct output but insert data into database    
    """
    # open song file
    df = pd.read_json(filepath, lines = True)
    
    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    print(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function process the log file in filepath with postgresql cursor. It first opens the file, retrieves and transformed desired data from it and finally insert into our database
    
    Input: Cursor and filepath
    Output: No direct output but insert data into database    
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    df['dt'] = pd.to_datetime(df['ts'], unit='ms')

    # convert timestamp column to datetime
    time_df = pd.DataFrame({"start_time":df['ts']})
    time_df['hour'] = pd.DatetimeIndex(df['dt']).hour
    time_df['day'] = pd.DatetimeIndex(df['dt']).day
    time_df['week_of_year'] = pd.DatetimeIndex(df['dt']).weekofyear
    time_df['month'] = pd.DatetimeIndex(df['dt']).month
    time_df['year'] = pd.DatetimeIndex(df['dt']).year
    time_df['weekday'] = pd.DatetimeIndex(df['dt']).weekday

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    df = df.sort_values(by='ts')
    user_df = df[['userId','firstName','lastName','gender','level']]

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
        songplay_data =  [int(row.ts), int(row.userId), row.level, songid, artistid, int(row.sessionId), row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function processes all the files under filepath and utilizes functions defined above to retrieve data and insert data into database.
    
    Input: Connection and cursor for postgresql as well as filepath to the files and functions for processing
    Output: No direct output but process all files under filepath  
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
            
    #Reorder log files from earlies to latest and get rid of duplicate log files to prevent update error for user_id and level
    if 'log' in filepath:
        all_files = reorder(all_files)
    
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
        
def reorder(file_list):
    """
    This function reorders the files according to their minimum timestamp. In order to  avoid update error of user_id and level.
    
    Input: Filepath list
    Output: Ordered filepath list
    """
    #Create reference dictionary and timestamp list
    ref_dict = {}
    tss = []
    i=0
    
    #Iterate through files to retrieve minimum timestamp
    for path in file_list:
        df_temp = pd.read_json(path, lines = True)
        ts = df_temp.sort_values(by='ts')['ts'][0]
        ref_dict[ts]=i
        tss.append(ts)
        i+=1
        
    #Sort the timestamp
    ts_sort = sorted(set(tss))
    new_order = []
    
    #Create new order based on sorted timestamp
    for item in ts_sort:
        new_order.append(file_list[ref_dict[item]])
        
    return new_order

def main():
    """
    This is the main function that schedules the data processing procedure with other functions defined in this script
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()