import pymssql


def get_mssql_conn():
    conn = pymssql.connect(server='', user='sa', password='', database='ExLogBook2')  
    
    return conn.cursor()

def callback(path, qs, self):
    return eval('%s(qs)' % path[1:])

def api_log_get_db(qs):
    d = []

    cursor = get_mssql_conn()

    cursor.execute('SELECT [Code], [Name] FROM [ExLogBook2].[dbo].[Infobases];')
    
    row = cursor.fetchone()  
    while row:  
        d.append({'code':row[0], 'name':row[1]})

        row = cursor.fetchone()  

    return sorted(d, key=lambda e: e['name'])

def api_log_get_meta_data(qs):
    d = []

    cursor = get_mssql_conn()

    cursor.execute(f"""SELECT [Code], [Name] FROM [ExLogBook2].[dbo].[Metadata] WHERE [InfobaseCode]={qs['db'][0]};""")  
    row = cursor.fetchone()  
    while row:  
        d.append({'code':row[0], 'name':row[1]})

        row = cursor.fetchone()  

    return d

def api_log_get_event_objs(qs):
    db = 1
    if not qs.get('db') is None:
        db = qs['db'][0]

    data_string = ''
    if not qs.get('data_string') is None:
        data_string = ' AND [DataString] LIKE \'%'+qs.get('data_string')[0]+'%\''

    metadata_id = ''
    if not qs.get('metadata_id') is None:
        metadata_id = ' AND [MetadataId] = '+qs.get('metadata_id')[0]

    date_start = ''
    if not qs.get('date_start') is None:
        date_start = ' AND [DateTime] >= CONVERT(DATETIME, \''+qs.get('date_start')[0]+' 00:00\', 103)'

    date_end = ''
    if not qs.get('date_end') is None:
        date_end = ' AND [DateTime] <= CONVERT(DATETIME, \''+qs.get('date_end')[0]+' 23:59\', 103)'

    d = []

    query_text = f"""SELECT DISTINCT [DataString], [DataStructure], [ref_ones] FROM [ExLogBook2].[dbo].[v_Events] WHERE [InfobaseCode]={db} {data_string} {metadata_id} {date_start} {date_end}"""

    cursor = get_mssql_conn()

    cursor.execute(query_text)  

    row = cursor.fetchone()  
    while row:  
        d.append({'data_string':row[0], 'data_structure':row[1], 'ref_ones':row[2]})

        row = cursor.fetchone()  

    return sorted(d, key=lambda e: e['data_string'])

def api_log_get_results(qs):
    db = 4
    if not qs.get('db') is None:
        db = qs['db'][0]

    data_string = ''
    if not qs.get('data_string') is None:
        data_string = ' AND [DataString] LIKE \'%'+qs.get('data_string')[0]+'%\''

    metadata_id = ''
    if not qs.get('metadata_id') is None:
        metadata_id = ' AND [MetadataId] = '+qs.get('metadata_id')[0]

    date_start = ''
    if not qs.get('date_start') is None:
        date_start = ' AND [DateTime] >= CONVERT(DATETIME, \''+qs.get('date_start')[0]+' 00:00\', 103)'

    date_end = ''
    if not qs.get('date_end') is None:
        date_end = ' AND [DateTime] <= CONVERT(DATETIME, \''+qs.get('date_end')[0]+' 23:59\', 103)'

    data_structure = ''
    if not qs.get('data_structure') is None:
        data_structure = ' AND [DataStructure] = '+qs.get('data_structure')[0]
   
    ref_ones = ''
    if not qs.get('ref_ones') is None:
        ref_ones = ' AND [ref_ones] = \''+qs.get('ref_ones')[0]+'\''

    d = []

    query_text = f"""SELECT [DateTime], [Comment], [DataString], [DataStructure], [UsersName], [ComputerName], [MetadataName], [EventTypeName], [ApplicationName], [EventType], [EventID], [TransactionStatus] FROM [ExLogBook2].[dbo].[v_Events] WHERE [InfobaseCode]={db} {data_structure} {data_string} {metadata_id} {ref_ones} {date_start} {date_end}"""

    cursor = get_mssql_conn()

    cursor.execute(query_text) 
    
    row = cursor.fetchone()  
    while row:  
        d.append({'date_time':row[0], 'comment':row[1], 'data_string':row[2], 'data_structure':row[3], 'user_name':row[4], 'computer_name':row[5], 'metadata_name':row[6], 'event_type_name':row[7], 'application_name':row[8], 'event_type':row[9], 'event_id':row[10], 'transaction_status':row[11]})

        row = cursor.fetchone()  

    return sorted(d, key=lambda e: e['date_time'])

def api_log_get_result_mod_zn(qs):
    d = []

    query_text = 'SELECT DISTINCT DataString, UsersName, mod_post_doc, ref_ones, var_1, var_2, var_3, var_4 FROM (SELECT [DateTime], [Comment], [DataString], [DataStructure], [UsersName], [ComputerName], [MetadataName], [EventTypeName], [ApplicationName], [EventType], [EventID], [TransactionStatus], CASE WHEN [Comment] like \'%'+'Статус проведения документа до изменения:Да'+'%\' THEN 1 ELSE 0 END AS mod_post_doc, [ref_ones], CASE WHEN [Comment] like \'%<Контрагент>:%\' AND [Comment] like \'%<Автомобиль>:%\'THEN 1 ELSE 0 END AS var_1, CASE WHEN [Comment] like \'%<Автомобиль>:%\'THEN 1 ELSE 0 END AS var_2, CASE WHEN [Comment] like \'%<Контрагент>:%\'THEN 1 ELSE 0 END AS var_3, CASE WHEN [Comment] like \'%Таблица товаров до записи:%\' THEN 1 ELSE 0 END AS var_4 FROM [ExLogBook2].[dbo].[v_Events] WHERE [InfobaseCode]=4 and ([Comment] like \'%'+'Измененнные реквизиты шапки до записи:'+'%\' or [Comment] like \'%+'+'Таблица товаров до записи:'+ '%\' or [Comment] like \'%'+'Статус проведения документа до изменения:%\') %metadata_id% %date_start% %date_end% ) AS T1 WHERE (T1.mod_post_doc=0 and (T1.var_1=1 or T1.var_2=1 or T1.var_3=1)) or (T1.mod_post_doc=1 and T1.var_4=1)  order by var_1 desc, var_2 desc, var_3 desc, var_4 desc '

    query_text = query_text.replace('%metadata_id%', ' AND [MetadataId] = 120')

    date_start = ''
    if qs.get('date_start'):
        date_start =' AND [DateTime] >= CONVERT(DATETIME, \''+qs.get('date_start')[0]+' 00:00\', 103)'

    query_text = query_text.replace('%date_start%', date_start)
    
    date_end = ''
    if qs.get('date_end'):
       date_end = ' AND [DateTime] <= CONVERT(DATETIME, \''+qs.get('date_end')[0]+' 23:59\', 103)'
        
    query_text = query_text.replace('%date_end%', date_end)
    
    cursor = get_mssql_conn()

    cursor.execute(query_text) 
    
    row = cursor.fetchone()  
    while row:  
        d.append({'data_string':row[0], 'user_name':row[1], 'mod_post_doc':row[2], 'ref_ones':row[3], 'var_1':row[4], 'var_2':row[5], 'var_3':row[6], 'var_4':row[7]})

        row = cursor.fetchone()  

    return d

def api_log_get_users(qs):
    d = []

    cursor = get_mssql_conn()

    cursor.execute(f"""SELECT [InfobaseCode] ,[Code] ,[Name] ,[Guid] FROM [ExLogBook2].[dbo].[Users] WHERE [InfobaseCode]={qs['db'][0]} ORDER BY [Name]""")  
    row = cursor.fetchone()  
    while row:  
        d.append({'code':row[1], 'name':row[2]})

        row = cursor.fetchone()  

    return d

def api_log_get_events(qs):
    d = []

    cursor = get_mssql_conn()

    cursor.execute(f"""SELECT [InfobaseCode], [Code], 
                        
     CASE WHEN dbo.EventsType.Name = '_$Session$_.Start' THEN 'Сеанс. Начало' WHEN dbo.EventsType.Name = '_$Session$_.Finish' THEN 'Сеанс. Завершение' WHEN dbo.EventsType.Name = '_$InfoBase$_.ConfigUpdate' THEN
                          'Информационная база. Изменение конфигурации' WHEN dbo.EventsType.Name = '_$InfoBase$_.DBConfigUpdate' THEN 'Информационная база. Изменение конфигурации  базы данных' WHEN dbo.EventsType.Name =
                          '_$InfoBase$_.EventLogSettingsUpdate' THEN 'Информационная база. Изменение  параметров журнала регистрации' WHEN dbo.EventsType.Name = '_$InfoBase$_.InfoBaseAdmParamsUpdate' THEN 'Информационная база. Изменение  параметров информационной базы'
                          WHEN dbo.EventsType.Name = '_$InfoBase$_.MasterNodeUpdate' THEN 'Информационная база. Изменение главного  узла' WHEN dbo.EventsType.Name = '_$InfoBase$_.RegionalSettingsUpdate' THEN 'Информационная база. Изменение  региональных установок'
                          WHEN dbo.EventsType.Name = '_$InfoBase$_.TARInfo' THEN 'Тестирование и исправление. Сообщение' WHEN dbo.EventsType.Name = '_$InfoBase$_.TARMess' THEN 'Тестирование и исправление. Предупреждение' WHEN
                          dbo.EventsType.Name = '_$InfoBase$_.TARImportant' THEN 'Тестирование и исправление. Ошибка' WHEN dbo.EventsType.Name = '_$Data$_.New' THEN 'Данные. Добавление' WHEN dbo.EventsType.Name = '_$Data$_.Update'
                          THEN 'Данные. Изменение' WHEN dbo.EventsType.Name = '_$Data$_.Delete' THEN 'Данные. Удаление' WHEN dbo.EventsType.Name = '_$Data$_.TotalsPeriodUpdate' THEN 'Данные. Изменение периода рассчитанных итогов'
                          WHEN dbo.EventsType.Name = '_$Data$_.Post' THEN 'Данные. Проведение' WHEN dbo.EventsType.Name = '_$Data$_.Unpost' THEN 'Данные. Отмена проведения' WHEN dbo.EventsType.Name = '_$User$_.New' THEN 'Пользователи. Добавление'
                          WHEN dbo.EventsType.Name = '_$User$_.Update' THEN 'Пользователи. Изменение' WHEN dbo.EventsType.Name = '_$User$_.Delete' THEN 'Пользователи. Удаление' WHEN dbo.EventsType.Name = '_$Job$_.Start' THEN
                          'Фоновое задание. Запуск' WHEN dbo.EventsType.Name = '_$Job$_.Succeed' THEN 'Фоновое задание. Успешное завершение' WHEN dbo.EventsType.Name = '_$Job$_.Fail' THEN 'Фоновое задание. Ошибка выполнения'
                          WHEN dbo.EventsType.Name = '_$Job$_.Cancel' THEN 'Фоновое задание. Отмена' WHEN dbo.EventsType.Name = '_$PerformError$_' THEN 'Ошибка выполнения' WHEN dbo.EventsType.Name = '_$Transaction$_.Begin' THEN
                          'Транзакция. Начало' WHEN dbo.EventsType.Name = '_$Transaction$_.Commit' THEN 'Транзакция. Фиксация' WHEN dbo.EventsType.Name = '_$Transaction$_.Rollback' THEN 'Транзакция. Отмена' WHEN dbo.EventsType.Name
                          = '_$Access$_.Access' THEN 'Объект.Доступ' ELSE dbo.EventsType.Name end FROM [ExLogBook2].[dbo].[EventsType] WHERE [InfobaseCode]={qs['db'][0]} """)  
    row = cursor.fetchone()  
    while row:  
        d.append({'code':row[1], 'name':row[2]})

        row = cursor.fetchone()  

    return d

def api_log_get_user_results(qs):
    db = 4
    if not qs.get('db') is None:
        db = qs['db'][0]

    event = ''
    if not qs.get('event') is None:
        if not qs.get('event')[0]=='0':
            event = ' AND [EventID] = '+qs.get('event')[0]

    user = ''
    if not qs.get('user') is None:
        user = f""" AND [UsersName] = '{qs.get('user')[0]}'"""

    date_start = ''
    if not qs.get('date_start') is None:
        date_start = ' AND [DateTime] >= CONVERT(DATETIME, \''+qs.get('date_start')[0]+' 00:00\', 103)'

    date_end = ''
    if not qs.get('date_end') is None:
        date_end = ' AND [DateTime] <= CONVERT(DATETIME, \''+qs.get('date_end')[0]+' 23:59\', 103)'

    d = []

    query_text = f"""SELECT [DateTime], [Comment], [DataString], [DataStructure], [UsersName], [ComputerName], [MetadataName], [EventTypeName], [ApplicationName], [EventType], [EventID], [TransactionStatus] FROM [ExLogBook2].[dbo].[v_Events] WHERE [InfobaseCode]={db} {event} {user} {date_start} {date_end}"""

    cursor = get_mssql_conn()

    cursor.execute(query_text) 
    
    row = cursor.fetchone()  
    while row:  
        d.append({'date_time':row[0], 'comment':row[1], 'data_string':row[2], 'data_structure':row[3], 'user_name':row[4], 'computer_name':row[5], 'metadata_name':row[6], 'event_type_name':row[7], 'application_name':row[8], 'event_type':row[9], 'event_id':row[10], 'transaction_status':row[11]})

        row = cursor.fetchone()  

    return sorted(d, key=lambda e: e['date_time'])