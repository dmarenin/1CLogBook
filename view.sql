SELECT     dbo.Events.DateTime, dbo.Events.DataString, dbo.Events.DataStructure, dbo.Events.Comment, dbo.Computers.Name AS ComputerName, 
                      dbo.Users.Name AS UsersName, dbo.Events.MetadataID, dbo.Metadata.Name AS MetadataName, dbo.Infobases.Name AS InfobasesName, dbo.Events.UserName, 
                      dbo.Events.ref_ones, dbo.Events.InfobaseCode, 
                      CASE WHEN dbo.EventsType.Name = '_$Session$_.Start' THEN 'Сеанс. Начало' WHEN dbo.EventsType.Name = '_$Session$_.Finish' THEN 'Сеанс. Завершение' WHEN
                       dbo.EventsType.Name = '_$InfoBase$_.ConfigUpdate' THEN 'Информационная база. Изменение конфигурации' WHEN dbo.EventsType.Name = '_$InfoBase$_.DBConfigUpdate'
                       THEN 'Информационная база. Изменение конфигурации  базы данных' WHEN dbo.EventsType.Name = '_$InfoBase$_.EventLogSettingsUpdate' THEN 'Информационная база. Изменение  параметров журнала регистрации'
                       WHEN dbo.EventsType.Name = '_$InfoBase$_.InfoBaseAdmParamsUpdate' THEN 'Информационная база. Изменение  параметров информационной базы' WHEN
                       dbo.EventsType.Name = '_$InfoBase$_.MasterNodeUpdate' THEN 'Информационная база. Изменение главного  узла' WHEN dbo.EventsType.Name = '_$InfoBase$_.RegionalSettingsUpdate'
                       THEN 'Информационная база. Изменение  региональных установок' WHEN dbo.EventsType.Name = '_$InfoBase$_.TARInfo' THEN 'Тестирование и исправление. Сообщение'
                       WHEN dbo.EventsType.Name = '_$InfoBase$_.TARMess' THEN 'Тестирование и исправление. Предупреждение' WHEN dbo.EventsType.Name = '_$InfoBase$_.TARImportant'
                       THEN 'Тестирование и исправление. Ошибка' WHEN dbo.EventsType.Name = '_$Data$_.New' THEN 'Данные. Добавление' WHEN dbo.EventsType.Name = '_$Data$_.Update'
                       THEN 'Данные. Изменение' WHEN dbo.EventsType.Name = '_$Data$_.Delete' THEN 'Данные. Удаление' WHEN dbo.EventsType.Name = '_$Data$_.TotalsPeriodUpdate'
                       THEN 'Данные. Изменение периода рассчитанных итогов' WHEN dbo.EventsType.Name = '_$Data$_.Post' THEN 'Данные. Проведение' WHEN dbo.EventsType.Name
                       = '_$Data$_.Unpost' THEN 'Данные. Отмена проведения' WHEN dbo.EventsType.Name = '_$User$_.New' THEN 'Пользователи. Добавление' WHEN dbo.EventsType.Name
                       = '_$User$_.Update' THEN 'Пользователи. Изменение' WHEN dbo.EventsType.Name = '_$User$_.Delete' THEN 'Пользователи. Удаление' WHEN dbo.EventsType.Name
                       = '_$Job$_.Start' THEN 'Фоновое задание. Запуск' WHEN dbo.EventsType.Name = '_$Job$_.Succeed' THEN 'Фоновое задание. Успешное завершение' WHEN dbo.EventsType.Name
                       = '_$Job$_.Fail' THEN 'Фоновое задание. Ошибка выполнения' WHEN dbo.EventsType.Name = '_$Job$_.Cancel' THEN 'Фоновое задание. Отмена' WHEN dbo.EventsType.Name
                       = '_$PerformError$_' THEN 'Ошибка выполнения' WHEN dbo.EventsType.Name = '_$Transaction$_.Begin' THEN 'Транзакция. Начало' WHEN dbo.EventsType.Name
                       = '_$Transaction$_.Commit' THEN 'Транзакция. Фиксация' WHEN dbo.EventsType.Name = '_$Transaction$_.Rollback' THEN 'Транзакция. Отмена' ELSE dbo.EventsType.Name
                       END AS EventTypeName, dbo.Applications.Name AS ApplicationName, dbo.Events.EventType, dbo.Events.EventID, dbo.Events.TransactionStatus
FROM         dbo.Events WITH (NOLOCK) LEFT OUTER JOIN
                      dbo.EventsType ON dbo.Events.EventID = dbo.EventsType.Code AND dbo.Events.InfobaseCode = dbo.EventsType.InfobaseCode LEFT OUTER JOIN
                      dbo.Applications ON dbo.Events.InfobaseCode = dbo.Applications.InfobaseCode AND dbo.Events.AppName = dbo.Applications.Code LEFT OUTER JOIN
                      dbo.Metadata ON dbo.Events.InfobaseCode = dbo.Metadata.InfobaseCode AND dbo.Events.MetadataID = dbo.Metadata.Code LEFT OUTER JOIN
                      dbo.Users ON dbo.Events.InfobaseCode = dbo.Users.InfobaseCode AND dbo.Events.UserName = dbo.Users.Code LEFT OUTER JOIN
                      dbo.Computers ON dbo.Events.ComputerName = dbo.Computers.Code AND dbo.Events.ComputerName = dbo.Computers.Code AND 
                      dbo.Events.InfobaseCode = dbo.Computers.InfobaseCode LEFT OUTER JOIN
                      dbo.Infobases ON dbo.Events.InfobaseCode = dbo.Infobases.Code