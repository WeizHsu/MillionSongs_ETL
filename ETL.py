#!/usr/bin/env python
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import sys, getopt
import pandas as pd
import os


def executeSqlFromFile(filename):
    # Open and read the file as a single buffer
    f = open(filename, 'r')
    sqlFile = f.read()
    f.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')
    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            print(command)
            cs.execute(command)
        except OperationalError:
            print ("Command skipped")

def ET():
    # set file path up
    cwd = os.getcwd()
    song_path = cwd + '/data/song_data'
    log_path = cwd + '/data/log_data/2018/11'
    # Load song_data
    song_df = pd.DataFrame()
    def traverseFiles(path):
        nonlocal song_df
        if path[-5:] == '.json' :
            tem_df = pd.read_json(path, lines=True)
            song_df = pd.concat([song_df,tem_df],axis=0,ignore_index=True)
        if path[-5:] != '.json' :
            tem = os.listdir(path)
            while len(tem) != 0 :
                t = tem.pop(0)
                if t[0] != '.' :
                    traverseFiles(path + '/' + t)
    traverseFiles(song_path)
    # extract for song dimension table
    song_d = song_df[['song_id', 'title', 'artist_id','year', 'duration']]
    song_d = song_d.drop_duplicates().reset_index(drop=True)
    song_d = song_d.rename(columns={'song_id':'SONG_ID','title':'TITLE',
                                'artist_id':'ARTIST_ID','year':'YEAR','duration':'DURATION'})
    # extract for artist dimension table
    artist_d = song_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_d = artist_d.drop_duplicates().reset_index(drop=True)
    artist_d = artist_d.rename(columns={'artist_id':'ARTIST_ID','artist_name':'NAME',
                                'artist_location':'LOCATION','artist_latitude':'LATITUDE','artist_longitude':'LONGITUDE'})
    # load log data
    log_df = pd.DataFrame()
    for file in os.listdir(log_path):
        tem_df = pd.read_json(log_path + '/' + file, lines=True)
        log_df = pd.concat([log_df,tem_df],axis=0,ignore_index=True)
    # filter data and convert time data from timestamp
    broken_down_log_df = log_df[log_df.page == 'NextSong']
    broken_down_log_df['ts'] = pd.to_datetime(broken_down_log_df['ts'], unit='ms')
    # extract for time dimension table
    time_d = pd.DataFrame({'START_TIME':broken_down_log_df.ts, 
                       'HOUR':broken_down_log_df.ts.dt.hour,
                       'DAY':broken_down_log_df.ts.dt.day, 
                       'WEEK':broken_down_log_df.ts.dt.weekofyear, 
                       'MONTH':broken_down_log_df.ts.dt.month, 
                       'YEAR':broken_down_log_df.ts.dt.year, 
                       'WEEKDAY':broken_down_log_df.ts.dt.weekday})
    time_d = time_d.drop_duplicates().reset_index(drop=True)
    # extract for user dimension table
    user_d = broken_down_log_df[['userId', 'firstName', 'lastName', 'gender', 'level','ts']]
    user_d = user_d.drop_duplicates().reset_index(drop=True)
    update_df = user_d['ts'].groupby(user_d['userId']).max()
    user_d = pd.merge(update_df,user_d,on=['userId','ts'])
    user_d = user_d[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_d['userId'] = user_d['userId'].astype(int)
    user_d = user_d.rename(columns={'userId':'USER_ID','firstName':'FIRST_NAME',
                                'lastName':'LAST_NAME','gender':'GENDER','level':'LEVEL'})
    # extract for fact table
    broken_down_log_df = broken_down_log_df.rename(columns={'artist':'artist_name','song':'title'})
    merge_df = pd.merge(song_df,broken_down_log_df,how='right',on=['artist_name','title'])
    merge_df = merge_df.drop_duplicates().reset_index(drop=True)
    songplays_f = pd.DataFrame({
    'SONGPLAY_ID':[i for i in range(1,len(list(merge_df.page))+1)], 
    'START_TIME':merge_df.ts, 
    'USER_ID':merge_df.userId, 
    'LEVEL':merge_df.level, 
    'SONG_ID':merge_df.song_id, 
    'ARTIST_ID':merge_df.artist_id, 
    'SESSION_ID':merge_df.sessionId, 
    'LOCATION':merge_df.location, 
    'USER_AGENT':merge_df.userAgent})
    songplays_f['USER_ID'] = songplays_f['USER_ID'].astype(int)
    songplays_f = songplays_f.drop_duplicates().reset_index(drop=True)

    return user_d,song_d,artist_d,time_d,songplays_f

def ETL(user, password, account):
    cnt = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
        )
    global cs
    cs = cnt.cursor()
    try:
        print("run SQLs")
        executeSqlFromFile("./create_tables.sql")

        user_d,song_d,artist_d,time_d,songplays_f = ET()

        successu, nchunksu, nrowsu, _ =write_pandas(cnt, user_d, 'USERS',quote_identifiers=False)
        print(str(successu) + ', ' + str(nchunksu) + ', ' + str(nrowsu))
        successs, nchunkss, nrowss, _ =write_pandas(cnt, song_d, 'SONGS',quote_identifiers=False)
        print(str(successs) + ', ' + str(nchunkss) + ', ' + str(nrowss))
        successa, nchunksa, nrowsa, _ =write_pandas(cnt, artist_d, 'ARTISTS',quote_identifiers=False)
        print(str(successa) + ', ' + str(nchunksa) + ', ' + str(nrowsa))
        successt, nchunkst, nrowst, _ =write_pandas(cnt, time_d, 'TIME',quote_identifiers=False)
        print(str(successt) + ', ' + str(nchunkst) + ', ' + str(nrowst))
        successf, nchunksf, nrowsf, _ =write_pandas(cnt, songplays_f, 'SONGPLAYS',quote_identifiers=False)
        print(str(successf) + ', ' + str(nchunksf) + ', ' + str(nrowsf))
    finally:
        cs.close()
    cnt.close()
    

# main function to start the process
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hu:p:a:")
    except getopt.GetoptError as err:
        print(err)
        print('main.py -u <user> -p <password> -a <account>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('main.py -u <user> -p <password> -a <account>')
            sys.exit()
        elif opt in ("-u"):
            user = arg
        elif opt in ("-p"):
            password = arg
        elif opt in ("-a"):
            account = arg
    ETL(user, password, account)


if __name__ == "__main__":
    main()