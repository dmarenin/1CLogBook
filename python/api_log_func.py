#import peewee

#import json
from datetime import datetime
from datetime import timedelta

#import os
#import orm.fields
#import gevent
#import threading
## from call import Call # TODO loopback!

#import inspect

## import asyncio

import pymssql, base64, sys, json, time

def get_mssql_conn():
    
    conn = pymssql.connect(server='itts', user='sa', password='Aa123456', database='ExLogBook2')  
    cursor = conn.cursor()  

    return cursor

def api_log_get_meta_data(qs):
   
    #res = (OrgCRM
    #    .select(OrgCRM.ref, OrgCRM.name)
    #    .where(OrgCRM.mark == False)
    #    .where(OrgCRM.show == True))

    #res = res.dicts()
    #return list(res) 
   
    d = []

    cursor = get_mssql_conn()

    cursor.execute('SELECT [Code],[Name] FROM [ExLogBook2].[dbo].[Metadata] WHERE [InfobaseCode]=1;')  

    row = cursor.fetchone()  
    while row:  
        
        d.append({'code':row[0],'name':row[1]})

        row = cursor.fetchone()  

    #return sorted(d, key=lambda e: e['name'])
    return d

def api_log_get_event_objs(qs):
    
    #in:
    #metadata_id
    #data_string
     
    d = []

    query_text = 'SELECT DISTINCT [DataString], [DataStructure], [ref_ones] FROM [ExLogBook2].[dbo].[v_Events] WHERE [InfobaseCode]=1 %data_string% %metadata_id% %date_start% %date_end%'

    if qs.get('data_string'):
        query_text = query_text.replace('%data_string%', ' AND [DataString] LIKE \'%'+qs.get('data_string')[0]+'%\'')
    else:
        query_text = query_text.replace('%data_string%', '')
        
    if qs.get('metadata_id'):
        query_text = query_text.replace('%metadata_id%', ' AND [MetadataId] = '+qs.get('metadata_id')[0])
    else:
        query_text = query_text.replace('%metadata_id%', '')
        
    if qs.get('date_start'):
        query_text = query_text.replace('%date_start%', ' AND [DateTime] >= CONVERT(DATETIME, \''+qs.get('date_start')[0]+' 00:00\', 103)')
    else:
        query_text = query_text.replace('%date_start%', '')
            
    if qs.get('date_end'):
        query_text = query_text.replace('%date_end%', ' AND [DateTime] <= CONVERT(DATETIME, \''+qs.get('date_end')[0]+' 23:59\', 103)')
    else:
        query_text = query_text.replace('%date_end%', '')
    
    cursor = get_mssql_conn()

    cursor.execute(query_text)  
    row = cursor.fetchone()  
    while row:  
        
        d.append(
            {
                #'date_time':'',
                #'comment':row[1],
                'data_string':row[0],
                'data_structure':row[1],
                #'user_name':row[4],
                #'computer_name':row[5]
                'ref_ones':row[2]
             }
            )

        row = cursor.fetchone()  

    return sorted(d, key=lambda e: e['data_string'])

def api_log_get_results(qs):
   
    #in:
    #data_structure
    #data_string    
    ##data_string

    d = []

    query_text = 'SELECT [DateTime], [Comment], [DataString], [DataStructure], [UsersName], [ComputerName], [MetadataName], [EventTypeName], [ApplicationName], [EventType], [EventID], [TransactionStatus] FROM [ExLogBook2].[dbo].[v_Events] WHERE [InfobaseCode]=1 %data_structure% %data_string% %metadata_id% %ref_ones% %date_start% %date_end%'

    if qs.get('data_string'):
        query_text = query_text.replace('%data_string%', ' AND [DataString] LIKE \'%'+qs.get('data_string')[0]+'%\'')
    else:
        query_text = query_text.replace('%data_string%', '')
    
    if qs.get('data_structure'):
        query_text = query_text.replace('%data_structure%', ' AND [DataStructure] = '+qs.get('data_structure')[0])
    else:
        query_text = query_text.replace('%data_structure%', '')         
        
    if qs.get('metadata_id'):
        query_text = query_text.replace('%metadata_id%', ' AND [MetadataId] = '+qs.get('metadata_id')[0])
    else:
        query_text = query_text.replace('%metadata_id%', '')
    
    if qs.get('ref_ones'):
        query_text = query_text.replace('%ref_ones%', ' AND [ref_ones] = \''+qs.get('ref_ones')[0]+'\'')
    else:
        query_text = query_text.replace('%ref_ones%', '') 
    
    if qs.get('date_start'):
        query_text = query_text.replace('%date_start%', ' AND [DateTime] >= CONVERT(DATETIME, \''+qs.get('date_start')[0]+' 00:00\', 103)')
    else:
        query_text = query_text.replace('%date_start%', '')
            
    if qs.get('date_end'):
        query_text = query_text.replace('%date_end%', ' AND [DateTime] <= CONVERT(DATETIME, \''+qs.get('date_end')[0]+' 23:59\', 103)')
    else:
        query_text = query_text.replace('%date_end%', '')
    
    cursor = get_mssql_conn()

    cursor.execute(query_text)  
    row = cursor.fetchone()  
    while row:  
        
        d.append(
            {
                'date_time':row[0],
                'comment':row[1],
                'data_string':row[2],
                'data_structure':row[3],
                'user_name':row[4],
                'computer_name':row[5],
                'metadata_name':row[6],
                'event_type_name':row[7],
                'application_name':row[8],
                'event_type':row[9],
                'event_id':row[10],
                'transaction_status':row[11]
             }
            )

        row = cursor.fetchone()  

    return sorted(d, key=lambda e: e['date_time'])

def callback(path, qs, self):

    res = eval('%s(qs)' % path[1:])

    return res

def api_log_get_result_mod_zn(qs):
   
    #in:
    #data_structure
    #data_string    
    ##data_string

    d = []

    query_text = 'SELECT DISTINCT DataString, UsersName, mod_post_doc, ref_ones, var_1, var_2, var_3, var_4 FROM (SELECT [DateTime], [Comment], [DataString], [DataStructure], [UsersName], [ComputerName], [MetadataName], [EventTypeName], [ApplicationName], [EventType], [EventID], [TransactionStatus], CASE WHEN [Comment] like \'%'+'Статус проведения документа до изменения:Да'+'%\' THEN 1 ELSE 0 END AS mod_post_doc, [ref_ones], CASE WHEN [Comment] like \'%<Контрагент>:%\' AND [Comment] like \'%<Автомобиль>:%\'THEN 1 ELSE 0 END AS var_1, CASE WHEN [Comment] like \'%<Автомобиль>:%\'THEN 1 ELSE 0 END AS var_2, CASE WHEN [Comment] like \'%<Контрагент>:%\'THEN 1 ELSE 0 END AS var_3, CASE WHEN [Comment] like \'%Таблица товаров до записи:%\' THEN 1 ELSE 0 END AS var_4 FROM [ExLogBook2].[dbo].[v_Events] WHERE [InfobaseCode]=1 and ([Comment] like \'%'+'Измененнные реквизиты шапки до записи:'+'%\' or [Comment] like \'%+'+'Таблица товаров до записи:'+ '%\' or [Comment] like \'%'+'Статус проведения документа до изменения:%\') %metadata_id% %date_start% %date_end% ) AS T1 WHERE (T1.mod_post_doc=0 and (T1.var_1=1 or T1.var_2=1 or T1.var_3=1)) or (T1.mod_post_doc=1 and T1.var_4=1)  order by var_1 desc, var_2 desc, var_3 desc, var_4 desc '

    query_text = query_text.replace('%metadata_id%', ' AND [MetadataId] = 12')

    if qs.get('date_start'):
        query_text = query_text.replace('%date_start%', ' AND [DateTime] >= CONVERT(DATETIME, \''+qs.get('date_start')[0]+' 00:00\', 103)')
    else:
        query_text = query_text.replace('%date_start%', '')
            
    if qs.get('date_end'):
        query_text = query_text.replace('%date_end%', ' AND [DateTime] <= CONVERT(DATETIME, \''+qs.get('date_end')[0]+' 23:59\', 103)')
    else:
        query_text = query_text.replace('%date_end%', '')
    
    cursor = get_mssql_conn()

    cursor.execute(query_text)  
    row = cursor.fetchone()  
    while row:  
        
        d.append(
            {
                #'date_time':row[0],
                #'comment':row[1],
                'data_string':row[0],
                #'data_structure':row[3],
                'user_name':row[1],
                #'computer_name':row[5],
                #'metadata_name':row[6],
                #'event_type_name':row[7],
                #'application_name':row[8],
                #'event_type':row[9],
                #'event_id':row[10],
                #'transaction_status':row[11],
                'mod_post_doc':row[2],
                'ref_ones':row[3],
                'var_1':row[4],
                'var_2':row[5],
                'var_3':row[6],
                'var_4':row[7]
             }
            )

        row = cursor.fetchone()  

    return d

def json_serial(obj):
  if isinstance(obj, (datetime, datetime.date)):
    return obj.isoformat()
  raise TypeError("Type is not serializable %s" % type(obj))
