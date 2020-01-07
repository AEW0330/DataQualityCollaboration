-- step 1.
select
   'MMTRTORT_D2' as TBNM
  ,ROW_NUMBER() over(order by newid()) AS uniq_no
  ,de1.ORDSEQNO as ordseqno
  ,de1.OPSEQNO as op_seqno
  ,de1.PATNO as patno
  ,de1.PATFG as patfg
  ,de1.MEDDATE as meddate
  ,de1.ORDDATE as orddate
  ,NULL AS actdt
  ,de1.ORDCODE as ord_cd
  ,de1.CNT as cnt
  ,de1.Day as day
  ,de1.ACTCNT as act_cnt
  ,de1.tot_outqty
  ,de1.MEDDEPT as meddept
  ,de1.ORDDR as orddr
  ,de1.WARDNO as wardno
  ,de1.ORDCLSTYP as ord_gb
  ,de1.DCYN as dc_yn
  ,de1.PRNACTYN as prn_yn
  ,null as act_yn
  ,case
    when de1.OPDATE is not null then 'Y'
    else 'N'
   end as op_yn
  ,de1.DCYN as cancel_yn
  ,case
            when de1.MEDDATE > dd1.DIEDATE then 'Y'
            when de1.ORDDATE is not null and de1.ORDDATE > dd1.DIEDATE then 'Y'
            when de1.EXECPLDT is not null and de1.EXECPLDT > dd1.DIEDATE then 'Y'
            when de1.REGTIME is not null and de1.REGTIME > dd1.DIEDATE then 'Y'
            else 'N'
         end as death_err
   ,case
            when de1.MEDDATE <= dp1.Btdt then 'Y'
            when de1.ORDDATE is not null and de1.ORDDATE <= dp1.Btdt then 'Y'
            when de1.EXECPLDT is not null and de1.EXECPLDT < dp1.Btdt then 'Y'
            when de1.REGTIME is not null and de1.REGTIME < dp1.Btdt then 'Y'
            else 'N'
         end as Brith_err
   ,case
            when de1.MEDDATE > de1.ORDDATE then 'Y'
            when de1.ORDDATE > de1.EXECPLDT or de1.ORDDATE > de1.OPDATE then 'Y'
            when de1.MEDDATE > de1.OPDATE  then 'Y'
            else 'N'
     end as date_err
   ,case
      when de1.Del_yn = 'Y' then 'N'
      else 'Y'
    end as include_yn

 into BYUN_META_1M.dbo.Dev_Device_step1

    from
    (select
      v.*
      ,(v.OUTQTY-v.RETNQTY) as tot_outqty
      ,case
          when v.DEVICE_STDT is not null then dateadd(day,v.Day,v.DEVICE_STDT)
          else null
       end as DEVICE_ENDT
      ,null as  DEVICE_ENDT_TM
 from
    (select
         a.PATNO  -- 환자ID
        ,a.PATFG   -- 내원구분
        ,a.MEDDATE   -- 진료/입원/수술(DSC)/응급실도착일시
        ,a.ORDDATE   -- 처방일자
        ,a.OPDATE
        ,a.orddate as DEVICE_STDT
        ,null  as DEVICE_STDT_TM
        ,a.ORDSEQNO   -- 처방순번
        ,a.ORDCLSTYP   -- 처방분류TYP(MM182)
        ,a.ORDCODE   -- 방코드
        ,a.EXECPLDT   -- 실시예정일자(처치)
        ,a.MEDDEPT   -- 진료과
        ,a.ORDDR  -- 처방의사
        ,case
            when a.CNT is null then 0
            else a.CNT
          end as CNT -- 횟수
        ,case
            when a.R_DAY is null or a.R_DAY = '0' then 1
            else a.R_DAY
           end as Day
         ,a.WARDNO  -- 병동
         ,a.MKFG
         ,a.PRNACTYN  -- PRN실시처방여부
         ,a.REMARK  -- 특기사항
         ,a.DCYN   -- D/C여부
         ,a.DCORDSEQ  -- D/C원처방번호
         ,a.REGTIME   -- 등록일시
         ,a.OPSEQNO
         ,a.ACTCNT
         ,a.OUTQTY
         ,case when a.RETNQTY is null then 0
              else a.RETNQTY
          end as RETNQTY
         ,CASE WHEN a.DCYN = 'Y' THEN 'Y'
            WHEN DCORDSEQ IS NOT NULL	THEN 'Y'
            WHEN a.MKFG NOT IN ('N', 'P') THEN 'Y'
			      WHEN a.PRNACTYN = 'Y' THEN 'Y'
            ELSE 'N'
           end as Del_yn
    from (select
                PATNO,ORDDATE,ORDSEQNO,MEDDATE,PATFG,MEDDEPT,WARDNO ,OPDATE,OPSEQNO ,ORDCLSTYP,ORDCODE,ORDDR,ANETHSTM, ANETHETM
                ,CHADR ,OPDR,CNT,R_DAY,EXECPLDT,EXECDR,PRNACTYN,REMARK,DCYN,DCORDSEQ
                ,MKFG,ACTCNT,OUTQTY,RTNFG,RETNQTY,REGTIME
                        from BYUN_SOURCE_DATA.dbo.MMTRTORT_Device ) as a
    )as v

    )as de1
   inner join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as dp1
                on dp1.PATNO = de1.PATNO
   left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as dd1
                on dd1.PATNO = de1.PATNO;
--> 551,958

-----------------------------
-----------------------------
--> Step 2. visit_mapping <--
-----------------------------
-----------------------------
select
    *
into BYUN_META_1M.dbo.Dev_device
from
(select d1.*, v1.uniq_no as visit_id  from
(select * from BYUN_META_1M.dbo.Dev_Device_step1
  where include_yn= 'Y' and patfg in ('H','O')) as d1
left join (select
                    uniq_no,patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG = 'O' and tbnm in ('AOOPDLST')) as v1
                on v1.patno = d1.PATNO and v1.visit_date = d1.MEDDATE and v1.med_dept = d1.MEDDEPT and v1.med_dr = d1.orddr

-- 입원
union all
select d1.*, v1.uniq_no as visit_id  from
(select * from BYUN_META_1M.dbo.Dev_Device_step1
  where include_yn= 'Y' and patfg in ('E','I','D')) as d1
left join (select
                    uniq_no, patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG in ('E','I','D') and tbnm in ('APIPDSLT')) as v1
                on v1.patno = d1.PATNO and v1.visit_time = d1.MEDDATE
                   and v1.med_dept = d1.MEDDEPT and v1.patfg = d1.patfg
union all
select d1.*, v1.uniq_no as visit_id  from
(select * from BYUN_META_1M.dbo.Dev_Device_step1
  where include_yn= 'Y' and patfg in ('G','M')) as d1
left join (select patno, visit_date, visit_time, dsch_date, dsch_time,med_dr, med_dept, uniq_no,cancel_yn, case when patfg ='S' then 'M' else patfg end as patfg
                       from BYUN_META_1M.dbo.Dev_visit
                        where PATFG in ('G','S')) as v1
                on v1.patno = d1.PATNO and (cast(v1.visit_date as date) = cast(d1.MEDDATE as date)) and v1.patfg = d1.patfg)v

--> [2019-10-29 19:10:52] 561,793 rows affected in 2 s 321 ms ( 9,835 rows removed) 