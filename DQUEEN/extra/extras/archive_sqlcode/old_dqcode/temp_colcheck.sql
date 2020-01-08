
select
                   is1.Table_name
                  ,is1.Column_name
                  ,is1.Ordinal_position
                  ,is1.Is_nullable
                  ,is1.Data_Type
                  ,is1.Character_maximum_length
                  ,is1.Numeric_precision
                  ,is1.Datetime_precision
                  ,is1.Collation_name
                  ,ir1.rows
          From  @DWdatabaseSchema.INFORMATION_SCHEMA.COLUMNS as is1
          left join (SELECT  o.name
                           , i.rows

                     FROM sysindexes i
                      INNER JOIN sysobjects o  ON i.id = o.id
                     WHERE i.indid < 2  AND  o.xtype = 'U'  ) as ir1 on is1.TABLE_NAME = ir1.name collate korean_wansung_ci_as
;
