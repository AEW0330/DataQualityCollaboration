   select

       *
       , CASE WHEN DCYN = 'Y'                    THEN  'Y'
         WHEN DCORDSEQ IS NOT NULL                 THEN  'Y'
         WHEN MKFG NOT IN ('N', 'P')                 THEN  'Y'
         WHEN DRUGFG = 4                   THEN  'Y'
         WHEN PRNACTYN = 'Y' THEN  'Y' -- and PRNACTYN is null
         WHEN PREORDYN = 'Y'                   THEN  'Y'
         ELSE 'N'
          END AS Del_YN
    from BYUN_SOURCE_DATA.dbo.MMMEDORT

--;
    -- 1. 외래실시내역+약처방
    select
          ROW_NUMBER() over(order by newid()) AS uniq_no
        , *
    into Byun_meta_1M.dbo.Dev_drug_step1
    from
      (select
           distinct
           * from
            ( select
                'MNOUTACT_MMMEDORT' as TBNM
                ,md1.PATNO as PATNO
                ,md1.PATFG as PATFG
                ,mo1.MEDDEPT as MEDDEPT
                ,mo1.ORDSEQNO as ORDSEQNO
                ,null as seq
                ,mo1.ORDCODE as ORDCODE
                ,md1.MEDDATE as MEDDATE
                ,mo1.ORDDATE as ORDDATE
                ,null as ADMDATE
                ,case when mo1.ACTDTTM is not null then mo1.ACTDTTM
                      else null
                      end as act_stdt -- acting datetime
                ,null as act_endt
                ,md1.R_DAY as R_DAY
                ,md1.METHODCD as MEDTHODCD
                ,case
                    when md1.CNT is null or md1.CNT = '0' then '1'
                    else md1.CNT
                    end  as CNT
                ,case
                    when mo1.CNT is null or mo1.CNT = '0' then '1'
                    else mo1.CNT
                    end as ACTCNT
                ,md1.PACKQTY as PACKQTY
                ,md1.PACKUNIT as PACKUNIT
                ,mo1.ORDDR as ORDDR
                ,md1.REMARK as REMARK
                ,md1.MKFG as MKFG
                ,md1.DRUGFG as DRUGFG
                ,md1.WARDNO as WARDNO
                ,md1.DSCDRGYN
                ,mo1.ACTYN as ACTYN
                ,mo1.ORDDELYN as ORDDELYN
                ,md1.PRNACTYN as PRNACTYN
                ,md1.DCYN as DCYN
                ,md1.DCORDSEQ as DCORDSEQ
                ,md1.PREORDYN as PREORDYN
                ,mo1.REJTTIME as REJTTIME
                , CASE WHEN md1.DCYN = 'Y' THEN  'Y'
                       WHEN md1.DCORDSEQ IS NOT NULL   THEN  'Y'
                       WHEN md1.MKFG NOT IN ('N', 'P')  THEN  'Y'
                       WHEN md1.DRUGFG = 4   THEN  'Y'
                       WHEN md1.PRNACTYN = 'Y' and (mo1.ACTYN is null) THEN  'Y'
                       WHEN mo1.ORDDELYN ='Y' then 'Y'
                       WHEN md1.PREORDYN  = 'Y' THEN  'Y'
                       ELSE 'N'
                  END AS Del_YN
                 from
                (select PATNO,PATFG,MEDDEPT,ORDSEQNO,ORDCODE,MEDDATE,ORDDATE,R_DAY,METHODCD,CNT,PACKQTY,DSCDRGYN
                ,PACKUNIT,ORDDR,REMARK,MKFG,DRUGFG,WARDNO,PRNACTYN,DCYN,DCORDSEQ,PREORDYN  from BYUN_SOURCE_DATA.dbo.MMMEDORT where patfg not in  ('I','D','E')) as md1
               left join   (select  PATNO ,PATFG, MEDDEPT,ORDDATE,ORDSEQNO ,ORDCODE
                                         ,ACTYN,ACTDTTM,ORDDR,CNT,REJTTIME,ORDDELYN
                   from BYUN_SOURCE_DATA.dbo.MNOUTACT where REJTTIME is null and ACTYN ='Y') as mo1
                on md1.PATNO = mo1.PATNO and mo1.ORDDATE = md1.ORDDATE and mo1.ORDSEQNO = md1.ORDSEQNO and mo1.ORDCODE = md1.ORDCODE

union all
-- 2. 병동실시내역+약처방
    select
         'MNWADACT_MMMEDORT' as TBNM
        ,dr1.PATNO as PATNO
        ,dr1.PATFG as PATFG
        ,dr1.MEDDEPT as MEDDEPT
        ,dr1.ORDSEQNO as ORDSEQNO
        ,ac1.EXECSEQ as seq
        ,dr1.ORDCODE as ORDCODE
        ,dr1.MEDDATE as MEDDATE
        ,dr1.ORDDATE as ORDDATE
        ,ac1.ADMTIME as ADMTIME
        ,coalesce(ac1.INDTTM,ac1.ACTINGRETM) as act_stdt
        ,ac1.ENDDTTM  as act_endt
        ,dr1.R_DAY as R_DAY
        ,dr1.METHODCD as METHODCD
                ,case
                    when dr1.CNT is null or dr1.CNT = '0' then '1'
                    else dr1.CNT
                    end  as CNT
                ,case
                    when ac1.CNT is null or ac1.CNT = '0' then '1'
                    else ac1.CNT
                    end as ACTCNT
        ,dr1.PACKQTY as PACKQTY
        ,dr1.PACKUNIT as PACKUNIT
        ,dr1.ORDDR as ORDDR
        ,dr1.REMARK as REMARK
        ,dr1.MKFG as MKFG
        ,dr1.DRUGFG as DRUGFG
        ,dr1.WARDNO as WARDNO
        ,dr1.DSCDRGYN
        ,ac1.ACTFG as ACTFG
        ,null as ORDDELYN
        ,dr1.PRNACTYN as PRNACTYN
        ,dr1.DCYN as DCYN
        ,dr1.DCORDSEQ as DCORDSEQ
        ,dr1.PREORDYN as PREORDYN
        ,ac1.REJTTIME as REJTTIME
        , CASE WHEN dr1.DCYN = 'Y' THEN  'Y'
               WHEN dr1.MKFG NOT IN ('N', 'P')  THEN  'Y'
               WHEN dr1.DCORDSEQ IS NOT NULL   THEN  'Y'
               WHEN dr1.DRUGFG = 4   THEN  'Y'
               WHEN dr1.PRNACTYN = 'Y' and (ac1.ACTFG is null) THEN  'Y'
               WHEN dr1.PREORDYN  = 'Y' THEN  'Y'
               ELSE 'N'
          END AS Del_YN

    from
         (select PATNO,PATFG,MEDDEPT,ORDSEQNO,ORDCODE,MEDDATE,ORDDATE,R_DAY,METHODCD,CNT,PACKQTY,DSCDRGYN
                ,PACKUNIT,ORDDR,REMARK,MKFG,DRUGFG,WARDNO,PRNACTYN,DCYN,DCORDSEQ,PREORDYN from BYUN_SOURCE_DATA.dbo.MMMEDORT where PATFG in ('I','E','D')
            ) as dr1
    left join   (select PATNO,ORDDATE,ORDSEQNO,EXECSEQ,ORDCODE,ORDCLSFG,ADMTIME
                    ,INDTTM,UPDTTM,ENDDTTM,ACTINGRETM,CNT,ACTFG,REJTTIME
                  from BYUN_SOURCE_DATA.dbo.MNWADACT
                    where ACTFG = 'Y' and REJTTIME is null ) as ac1
               on  dr1.PATNO = ac1.PATNO
                  and  dr1.ORDDATE  = ac1.ORDDATE
                    AND dr1.ORDSEQNO = ac1.ORDSEQNO
                      AND dr1.ORDCODE  = ac1.ORDCODE)v)w;
--> [2019-10-30 16:56:11] 6,352,115 rows affected in 32 s 842 ms

--> step 2 visit_id mapped seq
select
  m2.*
  ,case
            when m2.MEDDATE > dd1.death_dt then 'Y'
            when m2.ORDDATE is not null and m2.ORDDATE > dd1.death_dt then 'Y'
            when m2.act_stdt is not null and m2.act_stdt > dd1.death_dt then 'Y'
            when m2.act_endt is not null and m2.act_endt > dd1.death_dt then 'Y'
            else 'N'
         end as death_err
   ,case
            when m2.MEDDATE <= dp1.Btdt then 'Y'
            when m2.ORDDATE is not null and m2.ORDDATE <= dp1.Btdt then 'Y'
            when m2.act_stdt is not null and m2.act_stdt < dp1.Btdt then 'Y'
            when m2.act_endt is not null and m2.act_endt < dp1.Btdt then 'Y'
            else 'N'
         end as Brith_err
   ,case
            when m2.MEDDATE > m2.ORDDATE then 'Y'
            when m2.ORDDATE > m2.act_stdt or m2.ORDDATE > m2.act_endt then 'Y'
            when m2.MEDDATE > m2.act_stdt  then 'Y'
            else 'N'
     end as date_err
into Byun_meta_1M.dbo.Dev_drug
from
(select
     d1.*
    ,v1.uniq_no as visit_id
from
  (select
     TBNM as tbnm
    ,uniq_no
    ,ORDSEQNO as ordseq_no
    ,PATNO as patno
    ,PATFG as patfg
    ,MEDDEPT as meddept
    ,MEDDATE as meddate
    ,ORDDATE as orddate
    ,act_stdt as act_stdt
    ,act_endt as act_endt
    ,seq as exe_seq
    ,MEDTHODCD as methodcd
    ,ORDCODE as ord_cd
    ,R_DAY as day
    ,CNT as cnt
    ,null as addcnt
    ,ACTCNT as actcnt
    ,PACKQTY as packqty
    ,PACKUNIT as packunit
    ,MKFG as ordgb
    ,case
       when DRUGFG = 1 and DSCDRGYN ='Y' then 'self_discharge_drg'
       when DRUGFG != 1 and DSCDRGYN ='Y' then 'discharge_drg'
       when DRUGFG = 1 and (DSCDRGYN ='N' or DSCDRGYN is null) then 'self_drg'
      else null
      end as drg_gb
    ,REMARK as ord_memo
    ,null as stoptxt
    ,orddr as orddr
    ,WARDNO as wardno
    ,DCYN as dc_yn
    ,PRNACTYN as prn_yn
    ,Del_YN as cancel_yn
    ,ACTYN
    ,ORDDELYN
    ,DCORDSEQ
    ,PREORDYN
    ,REJTTIME
   from Byun_meta_1M.dbo.Dev_drug_step1
  where Del_YN = 'N' and PATFG in ('H','O')
  ) as d1
   left join
     (select
         uniq_no,patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
       from BYUN_META_1M.dbo.Dev_visit
          where PATFG = 'O' and tbnm in ('AOOPDLST')) as v1
      on v1.patno = d1.PATNO and v1.visit_date = d1.MEDDATE and v1.med_dept = d1.MEDDEPT and v1.med_dr = d1.orddr
union all
   select
     d1.*
    ,v1.uniq_no as visit_id
from
  (select
     TBNM as tbnm
    ,uniq_no
    ,ORDSEQNO as ordseq_no
    ,PATNO as patno
    ,PATFG as patfg
    ,MEDDEPT as meddept
    ,MEDDATE as meddate
    ,ORDDATE as orddate
    ,act_stdt as act_stdt
    ,act_endt as act_endt
    ,seq as exe_seq
    ,MEDTHODCD as methodcd
    ,ORDCODE as ord_cd
    ,R_DAY as day
    ,CNT as cnt
    ,null as addcnt
    ,ACTCNT as actcnt
    ,PACKQTY as packqty
    ,PACKUNIT as packunit
    ,MKFG as ordgb
    ,case
       when DRUGFG = 1 and DSCDRGYN ='Y' then 'self_discharge_drg'
       when DRUGFG != 1 and DSCDRGYN ='Y' then 'discharge_drg'
       when DRUGFG = 1 and (DSCDRGYN ='N' or DSCDRGYN is null) then 'self_drg'
      else null
      end as drg_gb
    ,REMARK as ord_memo
    ,null as stoptxt
    ,orddr as orddr
    ,WARDNO as wardno
    ,DCYN as dc_yn
    ,PRNACTYN as prn_yn
    ,Del_YN as cancel_yn
    ,ACTYN
    ,ORDDELYN
    ,DCORDSEQ
    ,PREORDYN
    ,REJTTIME
   from Byun_meta_1M.dbo.Dev_drug_step1
  where Del_YN = 'N' and PATFG in ('E','I','D')
  ) as d1
left join (select
               uniq_no, patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                from BYUN_META_1M.dbo.Dev_visit
                 where PATFG in ('E','I','D') and tbnm in ('APIPDSLT')) as v1
                on v1.patno = d1.PATNO and v1.visit_time = d1.MEDDATE
                   and v1.med_dept = d1.MEDDEPT and v1.patfg = d1.patfg
union all
    select
     d1.*
    ,v1.uniq_no as visit_id
from
  (select
     TBNM as tbnm
    ,uniq_no
    ,ORDSEQNO as ordseq_no
    ,PATNO as patno
    ,PATFG as patfg
    ,MEDDEPT as meddept
    ,MEDDATE as meddate
    ,ORDDATE as orddate
    ,act_stdt as act_stdt
    ,act_endt as act_endt
    ,seq as exe_seq
    ,MEDTHODCD as methodcd
    ,ORDCODE as ord_cd
    ,R_DAY as day
    ,CNT as cnt
    ,null as addcnt
    ,ACTCNT as actcnt
    ,PACKQTY as packqty
    ,PACKUNIT as packunit
    ,MKFG as ordgb
    ,case
       when DRUGFG = 1 and DSCDRGYN ='Y' then 'self_discharge_drg'
       when DRUGFG != 1 and DSCDRGYN ='Y' then 'discharge_drg'
       when DRUGFG = 1 and (DSCDRGYN ='N' or DSCDRGYN is null) then 'self_drg'
      else null
      end as drg_gb
    ,REMARK as ord_memo
    ,null as stoptxt
    ,orddr as orddr
    ,WARDNO as wardno
    ,DCYN as dc_yn
    ,PRNACTYN as prn_yn
    ,Del_YN as cancel_yn
    ,ACTYN
    ,ORDDELYN
    ,DCORDSEQ
    ,PREORDYN
    ,REJTTIME
   from Byun_meta_1M.dbo.Dev_drug_step1
  where Del_YN = 'N' and PATFG in ('G','M')
  ) as d1
   left join (select patno, visit_date, visit_time, dsch_date, dsch_time,med_dr, med_dept, uniq_no,cancel_yn, case when patfg ='S' then 'M' else patfg end as patfg
                       from BYUN_META_1M.dbo.Dev_visit
                        where PATFG in ('G','S')) as v1
                on v1.patno = d1.PATNO and (cast(v1.visit_date as date) = cast(d1.MEDDATE as date)) and v1.patfg = d1.patfg) as m2
    inner join (select PATNO, Btdt from Byun_meta_1M.dbo.Dev_person) as dp1
                on dp1.PATNO = m2.PATNO
   left join (select PATNO, death_dt from Byun_meta_1M.dbo.Dev_death) as dd1
                on dd1.PATNO = m2.PATNO;
--> [2019-10-30 19:22:58] 5,684,150 rows affected in 11 s 570 ms