Use @source_db
-- source
   select distinct
         'S' as Stage_gb
         ,it1.TABLE_CATALOG
         ,it1.TABLE_SCHEMA
         ,it1.TABLE_NAME
         ,it1.TABLE_TYPE
         ,rc1.rows
         ,ts1.table_size
         ,ts1.UNIT
into @result_database_schema.dbo.schema_capacity
   from INFORMATION_SCHEMA.TABLES as it1
       inner join (SELECT o.name
                        , i.rows
                       FROM sysindexes i
                         INNER JOIN
                         sysobjects o
                         ON i.id = o.id
                       WHERE i.indid < 2
                       AND  o.xtype = 'U'
                       ) as rc1
            on rc1.name = it1.TABLE_NAME
       left join (
                    SELECT
                           table_name = convert(varchar(30), min(o.name))
                          ,table_size = convert(int, ltrim(str(sum(cast(reserved as bigint)) * 8192 / 1024., 15, 0)))
                          ,UNIT = 'KB'
                     FROM sysindexes i
                  INNER JOIN sysobjects o  ON (o.id = i.id)
                    WHERE i.indid IN (0, 1, 255)  AND  o.xtype = 'U'
                       GROUP BY i.id
                  ) as ts1
             on it1.TABLE_NAME = ts1.table_name
-- Meta
Use @meta_db

insert into DQUEEN_Result.dbo.schema_capacity
     select distinct
         'M' as Stage_gb
         ,it1.TABLE_CATALOG
         ,it1.TABLE_SCHEMA
         ,it1.TABLE_NAME
         ,it1.TABLE_TYPE
         ,rc1.rows
         ,ts1.table_size
         ,ts1.UNIT
   from INFORMATION_SCHEMA.TABLES as it1
       inner join (SELECT o.name
                        , i.rows
                       FROM sysindexes i
                         INNER JOIN
                         sysobjects o
                         ON i.id = o.id
                       WHERE i.indid < 2
                       AND  o.xtype = 'U'
                       ) as rc1
            on rc1.name = it1.TABLE_NAME
       left join (
                    SELECT
                           table_name = convert(varchar(30), min(o.name))
                          ,table_size = convert(int, ltrim(str(sum(cast(reserved as bigint)) * 8192 / 1024., 15, 0)))
                          ,UNIT = 'KB'
                     FROM sysindexes i
                  INNER JOIN sysobjects o  ON (o.id = i.id)
                    WHERE i.indid IN (0, 1, 255)  AND  o.xtype = 'U'
                       GROUP BY i.id
                  ) as ts1
             on it1.TABLE_NAME = ts1.table_name ;

-- CDM
use CDMPv1

insert into DQUEEN_Result.dbo.schema_capacity
 select distinct
         'C' as Stage_gb
         ,it1.TABLE_CATALOG
         ,it1.TABLE_SCHEMA
         ,it1.TABLE_NAME
         ,it1.TABLE_TYPE
         ,rc1.rows
         ,ts1.table_size
         ,ts1.UNIT
   from INFORMATION_SCHEMA.TABLES as it1
       inner join (SELECT o.name
                        , i.rows
                       FROM sysindexes i
                         INNER JOIN
                         sysobjects o
                         ON i.id = o.id
                       WHERE i.indid < 2
                       AND  o.xtype = 'U'
                       ) as rc1
            on rc1.name = it1.TABLE_NAME
       left join (
                    SELECT
                           table_name = convert(varchar(30), min(o.name))
                          ,table_size = convert(int, ltrim(str(sum(cast(reserved as bigint)) * 8192 / 1024., 15, 0)))
                          ,UNIT = 'KB'
                     FROM sysindexes i
                  INNER JOIN sysobjects o  ON (o.id = i.id)
                    WHERE i.indid IN (0, 1, 255)  AND  o.xtype = 'U'
                       GROUP BY i.id
                  ) as ts1
             on it1.TABLE_NAME = ts1.table_name