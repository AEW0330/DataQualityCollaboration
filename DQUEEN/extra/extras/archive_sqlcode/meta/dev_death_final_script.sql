  select
   *
  ,case
      when birth_err = 'Y' or date_err = 'Y' then 'N'
      else 'Y'
      end as include_yn
into BYUN_META_1M.dbo.Dev_Death
  from
    (select
        'MMCERMST' as tbnm
        ,ROW_NUMBER() over(order by newid()) as uniq_no
        ,row_number() over(partition by d1.patno order by d1.patno) as seq
        ,d1.PATNO as patno
        ,d1.DIEDATE as death_dt  -- 사망시간
       , (case
            when d1.CERTYP = '2'
            then 'Death_certification'
            when d1.CERTYP = '3'
            then 'Cerificate_of_death_dead_body'
            when d1.CERTYP ='5'
            then 'Certificate_of_stillbirth'
            else null
         end ) cf_typ
        ,d1.DPLCTYPE as death_typ	  -- 사망(사산)종류
        ,d1.DIDEADRS as death_cause_im  -- 직접사인
        ,d1.MIDDEADRS	as death_cause_mid-- 중간선행사인
        ,d1.PREDEADRS	as death_cause_pre -- 선행사인
        ,case
          when cast(d1.DIEDATE as date) >= cast(getdate() as date) or cast(d1.DIEDATE as date) >= cast('2018-01-01' as date)
          then 'Y'
          else 'N'
         end as date_err
        ,case
          when cast(d1.DIEDATE as date) < cast(p1.Btdt as date)
          then 'Y'
          else 'N'
         end as birth_err
        ,d1.AETCDTP	  -- 외인사종류
        ,d1.NSBIRTH	  -- 자연사산원인
        ,MEDDATE	  --진료/입원/수술(DSC)/응급실도착일시
    from
         (select * from  BYUN_SOURCE_DATA.dbo.Death_cerification) as d1
        join BYUN_META_1M.dbo.Dev_person as p1 on p1.PATNO = d1.PATNO)v;
--> [2019-10-28 16:30:13] 1,113 rows affected in 119 ms

