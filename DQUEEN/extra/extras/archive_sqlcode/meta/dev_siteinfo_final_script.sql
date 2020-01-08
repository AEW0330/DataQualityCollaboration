
select
  *
 ,case
    when vld_gb = 'Y' or Date_err = 'N' then 'Y'
    else 'N'
  end as include_yn
into BYUN_META_1M.dbo.dev_siteinfo
from
  (select
         'Dept_master' as tbnm
        ,ROW_NUMBER() over(order by newid()) as uniq_no
        ,Dept_CD as site_cd
        ,Dept_nm_EN as site_name_en
        ,Dept_nm_KR as site_name_kr
        ,'16499' as zip_cd
        ,open_dt
        ,reg_dt
        ,use_fg as vld_gb
        ,(case
            when cast(open_dt as date) > cast(getdate() as date)
            then 'Y'
            else 'N'
          end) as Date_err
  --  into Byun_meta_2M.dbo.Dev_siteinfo
    from BYUN_SOURCE_DATA.dbo.Dept_master)v ;
--> [2019-10-28 17:33:37] 930 rows affected in 25 ms

    select * from Byun_Dqueen.dbo.schema_specification where stage_gb = 'meta' and tbnm = 'Dev_siteifo'