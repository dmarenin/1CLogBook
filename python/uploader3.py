import sqlite3
import pymssql
import datetime
import time
import _thread
import datetime


db_id = '85553150-64a7-444f-85f8-b74847ca3a21'
infobase_code = None
last_event_number = 0


def source_conn():
    try:
        #conn = sqlite3.connect('C:\\Users\\dmarenin.AUTOGRAD\\Desktop\\1Cv8 - копия.lgd')
        conn = sqlite3.connect('C:\\Program Files\\1cv8\\srvinfo\\reg_1541\\85553150-64a7-444f-85f8-b74847ca3a21\\1Cv8Log\\1Cv8.lgd')
        conn.row_factory = sqlite3.Row

        return conn

    except:
        return None
    pass

def dest_conn():
    try:
        conn = pymssql.connect(server='', user='sa', password='', database='ExLogBook2')

        return conn

    except:
        return None
    pass


#refernce
def upd_applications():
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor =dest_con.cursor(as_dict=True)

    sql_text = 'SELECT [code], [name] FROM [AppCodes]'
    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text = "IF NOT EXISTS (select * from [dbo].[Applications] where [Code] = %s AND [InfobaseCode] = %s) " + "INSERT INTO [dbo].[Applications] ([InfobaseCode],[Code],[Name]) VALUES(%s, %s, %s)"

        val = (d['code'], infobase_code, infobase_code, d['code'], d['name'])

        res = dest_cursor.execute(sql_text, val)

        dest_con.commit()

    dest_con.close()

    src_con.close()

def upd_computers():
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor =dest_con.cursor(as_dict=True)

    sql_text = 'SELECT [code], [name] FROM [ComputerCodes]'
    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text = "IF NOT EXISTS (select * from [dbo].[Computers] where [Code] = %s AND [InfobaseCode] = %s) " + "INSERT INTO [dbo].[Computers] ([InfobaseCode],[Code],[Name]) VALUES(%s, %s, %s)"

        val = (d['code'], infobase_code, infobase_code, d['code'], d['name'])

        res = dest_cursor.execute(sql_text, val)

        dest_con.commit()

    dest_con.close()

    src_con.close()

def upd_events_type():
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor =dest_con.cursor(as_dict=True)

    sql_text = 'SELECT [code], [name] FROM [EventCodes]'
    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text = "IF NOT EXISTS (select * from [dbo].[EventsType] where [Code] = %s AND [InfobaseCode] = %s) " + "INSERT INTO [dbo].[EventsType] ([InfobaseCode],[Code],[Name]) VALUES(%s, %s, %s)"

        val = (d['code'], infobase_code, infobase_code, d['code'], d['name'])

        res = dest_cursor.execute(sql_text, val)

        dest_con.commit()

    dest_con.close()

    src_con.close()

def upd_main_ports():
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor =dest_con.cursor(as_dict=True)

    sql_text = 'SELECT [code], [name] FROM [PrimaryPortCodes]'
    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text = "IF NOT EXISTS (select * from [dbo].[MainPorts] where [Code] = %s AND [InfobaseCode] =%s) " + "INSERT INTO [dbo].[MainPorts] ([InfobaseCode],[Code],[Name]) VALUES(%s, %s, %s)"

        val = (d['code'], infobase_code, infobase_code, d['code'], d['name'])

        res = dest_cursor.execute(sql_text, val)

        dest_con.commit()

    dest_con.close()

    src_con.close()

def upd_second_ports():
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor =dest_con.cursor(as_dict=True)

    sql_text = 'SELECT [code], [name] FROM [SecondaryPortCodes]'
    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text =  "IF NOT EXISTS (select * from [dbo].[SecondPorts] where [Code] = %s AND [InfobaseCode] = %s) " +"INSERT INTO [dbo].[SecondPorts] ([InfobaseCode],[Code],[Name]) VALUES(%s, %s, %s)"

        val = (d['code'], infobase_code, infobase_code, d['code'], d['name'])

        res = dest_cursor.execute(sql_text, val)

        dest_con.commit()

    dest_con.close()

    src_con.close()

def upd_servers():
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor =dest_con.cursor(as_dict=True)

    sql_text = 'SELECT [code], [name] FROM [WorkServerCodes]'
    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text = "IF NOT EXISTS (select * from [dbo].[Servers] where [Code] = %s AND [InfobaseCode] = %s) " +                                    "INSERT INTO [dbo].[Servers] ([InfobaseCode],[Code],[Name]) VALUES(%s, %s, %s)"

        val = (d['code'], infobase_code, infobase_code, d['code'], d['name'])

        res = dest_cursor.execute(sql_text, val)

        dest_con.commit()

    dest_con.close()

    src_con.close()

def upd_users():
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor =dest_con.cursor(as_dict=True)

    sql_text = 'SELECT [code], [uuid], [name] FROM [UserCodes]'
    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text = """MERGE INTO [dbo].[Users] AS Target 
                      USING (SELECT %s AS [Code], 
                                    %s AS [InfobaseCode], 
                                    %s AS [Guid]) AS Source 
                      ON (Target.[Code] = Source.[Code] 
                          AND Target.[InfobaseCode] = Source.[InfobaseCode] 
                          AND Target.[Guid] = Source.[Guid]) 

                      WHEN MATCHED AND NOT ([Name] = %s) THEN 
                           UPDATE  SET [Name] = %s

                      WHEN NOT MATCHED THEN 
                           INSERT ([InfobaseCode], [Code], [Name], [Guid]) VALUES (%s, %s, %s, %s);"""

        val = (d['code'], infobase_code, d['uuid'], d['name'], d['name'], infobase_code, d['code'], d['name'], d['uuid'])

        res = dest_cursor.execute(sql_text, val)

        dest_con.commit()

    dest_con.close()

    src_con.close()

def upd_meta_data():
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor =dest_con.cursor(as_dict=True)

    sql_text = 'SELECT [code], [uuid], [name] FROM [MetadataCodes]'
    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text = """MERGE INTO [dbo].[Metadata] AS Target
                      USING (SELECT %s AS [Code], 
                                    %s AS [InfobaseCode], 
                                    %s AS [Guid]) AS Source 
                      ON (Target.[Code] = Source.[Code] 
                          AND Target.[InfobaseCode] = Source.[InfobaseCode] 
                          AND Target.[Guid] = Source.[Guid]) 

                      WHEN MATCHED AND NOT ([Name] = %s) THEN 
                           UPDATE  SET [Name] = %s

                      WHEN NOT MATCHED THEN 
                           INSERT ([InfobaseCode], [Code], [Name], [Guid]) VALUES (%s, %s, %s, %s);"""

        val = (d['code'], infobase_code, d['uuid'], d['name'], d['name'], infobase_code, d['code'], d['name'], d['uuid'])

        res = dest_cursor.execute(sql_text, val)

        dest_con.commit()

    dest_con.close()

    src_con.close()

def upd_events(last_event_number):
    src_con = source_conn()
    if src_con is None:
        return
    src_cursor = src_con.cursor()

    dest_con = dest_conn()
    if dest_con is None:
        return
    
    dest_cursor = dest_con.cursor(as_dict=True)

    sql_text = f"""
    SELECT 
    [rowID],
    [severity],
    [date],
    [connectID],
    [session],
    [transactionStatus],
    [transactionDate],
    [transactionID],
    [userCode],
    [computerCode],
    [appCode],
    [eventCode],
    [comment],
    [metadataCodes],
    [sessionDataSplitCode],
    [dataType],
    [data],
    [dataPresentation],
    [workServerCode],
    [primaryPortCode],
    [secondaryPortCode],
    CASE
    WHEN  instr(data, ':')>0 
    THEN
    substr(data, 25 + instr(data, ':'), 8) || '-' ||
    substr(data, 21 + instr(data, ':'), 4) || '-' ||
    substr(data, 17 + instr(data, ':'), 4) || '-' || 
    substr(data, 1 + instr(data, ':'), 4)  || '-' ||
    substr(data, 5 + instr(data, ':'), 12)
    ELSE
    ''
    END  as ref_ones
    FROM [EventLog] 
    WHERE [rowID] > {last_event_number}
    ORDER BY rowID ASC
    LIMIT 100000"""

    src_cursor.execute(sql_text)
    result = src_cursor.fetchall()

    for r in result:
        d = dict(r)

        sql_text = """
        INSERT INTO [dbo].[Events]
           ([InfobaseCode]
           ,[DateTime]
           ,[TransactionStatus]
           ,[TransactionStartTime]
           ,[TransactionMark]
           ,[Transaction]
           ,[UserName]
           ,[ComputerName]
           ,[AppName]
           ,[EventID]
           ,[EventType]
           ,[Comment]
           ,[MetadataID]
           ,[DataStructure]
           ,[DataString]
           ,[ServerID]
           ,[MainPortID]
           ,[SecondPortID]
           ,[Seance]
           ,[ref_ones])
        VALUES(%s, CONVERT(DATETIME, %s, 20), %s, CONVERT(DATETIME, %s, 20), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        date_time = (datetime.datetime(1, 1, 1, 1)+ datetime.timedelta(seconds=int(d['date']/10000))).strftime("%Y-%m-%d %H:%M:%S")

        tran_start_time = (datetime.datetime(2001, 1, 1, 1)).strftime("%Y-%m-%d %H:%M:%S")
        if d['transactionDate']>0:
            tran_start_time =(datetime.datetime(1, 1, 1, 1)+ datetime.timedelta(seconds=int(d['transactionDate']/10000))).strftime("%Y-%m-%d %H:%M:%S")

        metadata_id = 0
        if len(d['metadataCodes'])>0:
            if ',' in d['metadataCodes']:
                print(d['metadataCodes'])
            else:
                try:
                    metadata_id = int(d['metadataCodes'])
                except:
                    pass
                        

        val = (infobase_code, date_time, d['transactionStatus'], tran_start_time, d['transactionID'], None, d['userCode'], d['computerCode'], d['appCode'], d['eventCode'], '', d['comment'], metadata_id, d['dataPresentation'], d['dataPresentation'], d['workServerCode'], d['primaryPortCode'], d['secondaryPortCode'], d['session'], d['ref_ones'])

        res = dest_cursor.execute(sql_text, val)

        last_event_number = d['rowID']

    dest_con.commit()

    print(str(datetime.datetime.now()) +' -> ' + str(last_event_number))


    src_con.execute('PRAGMA journal_mode = TRUNCATE')

    src_cursor = src_con.cursor()
        
    sql_text = f"""DELETE From EventLog WHERE rowID < (?)"""

    try:
        src_cursor.execute('''DELETE From EventLog WHERE rowID < ?''', (last_event_number,))
    except:
        pass

    #src_cursor.execute(sql_text)

    src_con.commit()

    sql_text = f"""DELETE from EventLogMetadata WHERE EventLogMetadata.eventLogID < (?)"""

    try:
        src_cursor.execute('''DELETE from EventLogMetadata WHERE EventLogMetadata.eventLogID < ?''', (last_event_number,))
    except:
        pass

    #src_cursor.execute(sql_text)

    src_con.commit()

    src_con.close()

def get_infobase_code():
    dest_con = dest_conn()
    if dest_con is None:
        return
    dest_cursor = dest_con.cursor(as_dict=True)

    sql_text = f"""SELECT [Guid],[Code],[Name] FROM [ExLogBook2].[dbo].[Infobases] WHERE Guid = '{db_id}'"""

    dest_cursor.execute(sql_text)

    result = dest_cursor.fetchall()

    if len(result)==0:
        return None

    return result[0]['Code']

def upd_refs():
    while True:
        time.sleep(180)

        upd_applications()
        upd_computers()
        upd_events_type()
        upd_main_ports()
        upd_second_ports()
        upd_servers()
        upd_users()
        upd_meta_data()

infobase_code = get_infobase_code()
if infobase_code is None:
    raise 'infobase_code is None'

last_event_number = 0

_thread.start_new_thread(upd_refs, ())

while True:
    time.sleep(30)

    upd_events(last_event_number)

