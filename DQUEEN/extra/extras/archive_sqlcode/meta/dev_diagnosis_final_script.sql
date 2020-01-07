select
         tm1.tbnm as tbnm
        ,ROW_NUMBER() over(order by newid()) AS uniq_no
        ,tm1.SEQNO as d_seq
        ,tm1.PATNO as patno
        ,tm1.MEDDATE as meddate
        ,tm1.visit_time as meddatetime
        ,null as other_dt
        ,tm1.dsch_date as dschdate
        ,tm1.dsch_time as dschdatetime
        ,tm1.MEDDEPT as meddept
        ,tm1.chadr as meddr
        ,tm1.patfg as patfg
        ,tm1.diagcode as diagcode
        ,tm1.mainyn as main_yn
        ,tm1.IMPRESSYN as impress_yn
        ,tm1.ADMDIAYN as admdia_yn
        ,tm1.dscdiayn as dscdia_yn
        ,null as ro_yn
        ,visit_id
        ,case
            when tm1.MEDDATE > dd1.DIEDATE then 'Y'
            when tm1.visit_time is not null and tm1.visit_time > dd1.DIEDATE then 'Y'
            when tm1.dsch_date is not null and tm1.dsch_date > dd1.DIEDATE then 'Y'
            when tm1.dsch_time is not null and tm1.dsch_time > dd1.DIEDATE then 'Y'
            else 'N'
         end as death_err
        ,case
            when tm1.MEDDATE <= dp1.Btdt then 'Y'
            when tm1.visit_time is not null and tm1.visit_time <= dp1.Btdt then 'Y'
            when tm1.dsch_date is not null and tm1.dsch_date < dp1.Btdt then 'Y'
            when tm1.dsch_time is not null and tm1.dsch_time < dp1.Btdt then 'Y'
            else 'N'
         end as birth_err
        ,case
            when tm1.MEDDATE > tm1.dsch_date then 'Y'
            when (tm1.visit_time is not null and tm1.dsch_time is not null) and tm1.visit_time > tm1.dsch_time then 'Y'
            else 'N'
         end as date_err
        ,tm1.cancel_yn
        ,case
           when tm1.cancel_yn = 'Y' then 'N'
           else 'Y'
         end as include_yn
  into Byun_meta_1M.dbo.Dev_Diagnosis
    from
    ( -- 외래, 가정간호
    select distinct
          'MMPDIAGT' as tbnm
        , m1.PATNO  , --환자ID
         m1.MEDDATE  , --진료/입원/수술(DSC)/응급실도착일시
         v1.visit_time
        , v1.dsch_date
        , v1.dsch_time
        , m1.MEDDEPT  , --진료과
         m1.PATFG  , --내원구분
         m1.CHADR  , --주치의
         m1.SEQNO  , --순번
         m1.DIAGCODE  , --진단코드
         m1.DIAGNAME  , --진단명
         m1.MAINYN  , --주진단여부
         m1.IMPRESSYN  , --확정진단여부
         m1.ADMDIAYN  , --입원진단여부
         m1.DSCDIAYN  , --퇴원진단여부
         m1.REGTIME , --등록일시
         v1.cancel_yn
        , v1.uniq_no  as visit_id

        from
             (select
                 PATNO, MEDDATE, MEDDEPT, PATFG, CHADR, SEQNO, DIAGCODE, DIAGNAME, EXTCDYN,
                 MAINYN, IMPRESSYN, COMDISYN, ADMDIAYN, DSCDIAYN, REGTIME, OUTYN, POACODE, ACUTYN
              from BYUN_SOURCE_DATA.dbo.MMPDIDAGT
                where PATFG in ('H','O')) as m1
        left join (select
                    uniq_no,patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG = 'O' and tbnm in ('AOOPDLST')) as v1
                on v1.patno = m1.PATNO and v1.visit_date = m1.MEDDATE and v1.med_dept = m1.MEDDEPT and v1.med_dr =m1.CHADR


     -- 응급/입원/통원수술
    union all
      select distinct
          'MMPDIAGT' as tbnm
        , m1.PATNO  , --환자ID
         m1.MEDDATE  , --진료/입원/수술(DSC)/응급실도착일시
         v1.visit_time
        , v1.dsch_date
        , v1.dsch_time
        , m1.MEDDEPT  , --진료과
         m1.PATFG  , --내원구분
         m1.CHADR  , --주치의
         m1.SEQNO  , --순번
         m1.DIAGCODE  , --진단코드
         m1.DIAGNAME  , --진단명
         m1.MAINYN  , --주진단여부
         m1.IMPRESSYN  , --확정진단여부
         m1.ADMDIAYN  , --입원진단여부
         m1.DSCDIAYN  , --퇴원진단여부
         m1.REGTIME , --등록일시
         v1.cancel_yn
        , v1.uniq_no as visit_id

        from
             (select
                 PATNO, MEDDATE, MEDDEPT, PATFG, CHADR, SEQNO, DIAGCODE, DIAGNAME, EXTCDYN,
                 MAINYN, IMPRESSYN, COMDISYN, ADMDIAYN, DSCDIAYN, REGTIME, OUTYN, POACODE, ACUTYN
              from BYUN_SOURCE_DATA.dbo.MMPDIDAGT
                where PATFG in ('E','I','D')) as m1
        left join (select
                    uniq_no, patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG in ('E','I','D') and tbnm in ('APIPDSLT')) as v1
                on v1.patno = m1.PATNO and v1.visit_time = m1.MEDDATE
                   and v1.med_dept = m1.MEDDEPT and v1.patfg = m1.patfg
    union all  --퇴원진단(입원)+입원환자방문내역
         select distinct
              'SMDDIAGT' as tbnm
             ,s1.PATNO   -- 환자ID
             ,v1.visit_date as MEDDATE
             ,v1.visit_time
             ,s1.DSCHDATE as dsch_date  -- 퇴원일
             ,v1.dsch_time
             ,s1.DEPTCODE as MEDDEPT   -- 부서코드
             ,v1.patfg
             ,case
                when s1.CHADR is not null then s1.CHADR
                when s1.CHADR is null then v1.med_dr
                else null
              end as cha_dr
             ,s1.SEQNO   -- 순번
             ,s1.DIAGCODE   -- 진단코드
             ,s1.DIAGNAME
             ,s1.MAINYN   -- 주코드여부
             ,null as IMPRESSYN
             ,null as ADMDIAYN
             , 'O' as DSCDIAYN
             ,s1.REGTIME   -- 생성일시
             ,v1.cancel_yn
             ,v1.uniq_no as visit_id
        from
             (select
                    PATNO,DSCHDATE,SEQNO,DIAGCODE,DIAGNAME
                    ,DEPTCODE,MAINYN,CHADR,REGTIME  from BYUN_SOURCE_DATA.dbo.SMDDIAGT) as s1
        join
                 ( select  patno, patfg,visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, uniq_no,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where tbnm = 'APIPDSLT' and PATFG in ('I','D')) as v1
        on s1.PATNO = v1.patno and s1.DEPTCODE = v1.med_dept and s1.DSCHDATE =  v1.dsch_time

    -- 검진, 산업의학
    union all
        select distinct
             'Gumjin+Sanup' as tbnm
            , m1.PATNO  , --환자ID
             m1.MEDDATE  , --진료/입원/수술(DSC)/응급실도착일시
             v1.visit_time
            , v1.dsch_date
            , v1.dsch_time
            , m1.MEDDEPT  , --진료과
             m1.PATFG  , --내원구분
             m1.CHADR  , --주치의
             m1.SEQNO  , --순번
             m1.DIAGCODE  , --진단코드
             m1.DIAGNAME  , --진단명
             m1.MAINYN  , --주진단여부
             m1.IMPRESSYN  , --확정진단여부
             m1.ADMDIAYN  , --입원진단여부
             m1.DSCDIAYN  , --퇴원진단여부
             m1.REGTIME , --등록일시
             v1.cancel_yn,
             v1.uniq_no as visit_id

            from
                 (select
                     PATNO, MEDDATE, MEDDEPT, case when PATFG = 'G' then 'G' when PATFG = 'M' then 'S' else PATFG end as PATFG, CHADR, SEQNO, DIAGCODE, DIAGNAME, EXTCDYN,
                     MAINYN, IMPRESSYN, COMDISYN, ADMDIAYN, DSCDIAYN, REGTIME, OUTYN, POACODE, ACUTYN
                  from BYUN_SOURCE_DATA.dbo.MMPDIDAGT
                    where PATFG in ('G','M')) as m1
            left join (select
                        patno, visit_date, visit_time, dsch_date, dsch_time,med_dr, med_dept, uniq_no,cancel_yn
                       from BYUN_META_1M.dbo.Dev_visit
                        where PATFG in ('G','S','O')) as v1
                on v1.patno = m1.PATNO and (cast(v1.visit_date as date) = cast(m1.MEDDATE as date)) ) as tm1

            inner join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as dp1
                on dp1.PATNO = tm1.PATNO
            left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as dd1
                on dd1.PATNO = tm1.PATNO
--> [2019-10-29 14:52:00] 1,309,733 rows affected in 6 s 701 ms


