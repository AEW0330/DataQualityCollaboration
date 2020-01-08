/*************************************************************************/
--  Assigment: DataQueen project
--  Description: Running script for Meta Person
--  Author: Junghyun E, Byun
--  Date:  7. May, 2019
--  Job name: Random sampling Original data (EHR)
--  Language: MSSQL
--  Target data: Meta
/*************************************************************************/

    select
         Seq as uniq_num
        ,Dept_CD as site_cd
        ,Dept_nm_EN as site_name_En
        ,Dept_nm_KR as site_name_kr
        ,'16499' as zip_cd
        ,Use_fg as vld_gb
        ,Open_dt
        ,Reg_dt
        ,(case
            when cast(open_dt as date) > cast(getdate() as date)
            then 'Y'
            else 'N'
          end) as Date_error
   into Byun_meta_2M.dbo.Dev_siteinfo
    from Byun_origin_Rand_2M.dbo.Dept_master
       -- where USe_fg = 'Y';
    -- [2019-05-07 21:12:33] 930 rows affected in 13 ms