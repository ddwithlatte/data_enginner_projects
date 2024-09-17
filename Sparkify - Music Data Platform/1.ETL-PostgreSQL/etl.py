import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from create_tables import main as create_table_main

def process_song_file(cur, filepath):
    """
    Process songs files and insert records into the Postgres database.
    :param cur: cursor reference
    :param filepath: complete file path for the file to load
    """

    # open song file
    df = pd.DataFrame([pd.read_json(filepath, typ='series', convert_dates=False)])

    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

        # insert artist record
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artist_data)

        # insert song record
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)
    
    print(f"Records inserted for file {filepath}")

def process_log_file(cur,filepath):

    df  = pd.read_json(filepath,lines= True) #true: 每一行都是一个独立的json

    df = df[df['page'] == "NextSong"].astype({'ts' : 'datetime64[ms]'}) 

    t = pd.Series(df['ts'] , index = df.index)

    # insert time data records 

    column_labels = ["timestamp", "hour", "day", "weelofyear", "month", "year", "weekday"]
    time_data = []

    for data in t:
         time_data.append([data ,data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])
    time_df = pd.DataFrame.from_records(data = time_data, columns = column_labels)
    

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert,list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert,row)
    
    #insert songplay records 
    for index,row in df.iterrows():
        cur.execute(song_select,(row.artist,row.song,row.length))
        results = cur.fetchone()

        if results:
            songid,artist = results 
        else :
            songid, artistid = None, None

        songplay_data = ( row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur,conn,filepath,func):

    all_files = []
    for root,firs,files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    #get total number of files found 
    num_files = len(all_files)
    print ('{} files found in {}'.format(num_files,filepath))

    #iterate over files found
    for i,datefile in enumerate(all_files,1):
        func(cur,datefile)
        conn.commit()
        print ('{}/{} files processed.'.format(i,num_files))


def main():
     conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=yuli password=admin")
     cur = conn.cursor()

     process_data(cur, conn, filepath='data/song_data', func=process_song_file)
     process_data(cur, conn, filepath='data/log_data', func=process_log_file)

     conn.close()



if __name__ == "__main__":
    create_table_main()
    main()
    print("\n\nFinished processing!!!\n\n")









