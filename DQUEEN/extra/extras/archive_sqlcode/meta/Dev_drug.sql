/*************************************************************************/
--  Assigment: DataQueen project
--  Description: Running script for Meta Person
--  Author: Junghyun E, Byun
--  Date:  13. May, 2019
--  Job name: Random sampling Original data (EHR)
--  Language: MSSQL
--  Target data: Meta
--  Description
-- 병동실시내역, 외래실시내역 테이블과 약처방 테이블 조인하여 사용 필요
-- 기존 약처방 테이블 내 Acting 확인 후 삭제 진행 해야함
-- 그 후 메타 생성
/*************************************************************************/
        -- SELECT COUNT(*) FROM Byun_origin_Rand_2M.dbo.MMMEDORT_BK;
            -- 115,107,174
-- 1. 외래 실시내역

    select * from Byun_origin_Rand_2M.dbo.MNOUTACT;
    select * from Byun_origin_Rand_2M.dbo.MMMEDORT_1;
    select
       PATNO -- 환자번호
      ,PATFG
      ,MEDDEPT -- 진료과
      ,ORDDATE -- 처방일자
      ,ORDSEQNO -- 처방순번
      ,EXECSEQNO -- 실시순번
      ,ORDTYP -- 처방형태
      ,ORDKIND -- 처방종류
      ,ORDCODE -- 처방코드
      ,ACTYN -- 실시여부
      ,ACTDTTM -- 실시일시
      ,ACTNEED -- 실시소요시간
      ,ORDDR -- 처방의사
      ,PATFG -- 내원구분
      ,DRUGCLS -- 약품분류(SD001)
      ,PACKQTY -- 투여량(포장단위)
      ,CNT -- 횟수
      ,REJTTIME -- 취소일시
      ,REQDATE -- 청구일자(원무)
      ,METHODCD -- """용법코드(내복약:MM011,주사제:MM012,외용약:MM013)"""
      ,PRNACTYN -- PRN유무
            from Byun_origin_Rand_2M.dbo.MNOUTACT;
   select

       *
       , CASE WHEN DCYN = 'Y'                    THEN  'Y'
         WHEN DCORDSEQ IS NOT NULL          THEN  'Y'
         WHEN MKFG NOT IN ('N', 'P')                 THEN  'Y'
         WHEN DRUGFG = 4                   THEN  'Y'
         WHEN PRNACTYN = 'Y'  THEN  'Y' --and prn_act_yn is null THEN  'Y'
         WHEN PREORDYN = 'Y'  THEN  'Y'
         ELSE 'N'
          END AS Del_YN
    from Byun_origin_Rand_2M.dbo.MMMEDORT_1

    select count(*)  from Byun_origin_Rand_2M.dbo.MMMEDORT_1
      where include_yn = 'N';
    -- 113,830,884
    --  89,720,379

    select * from DW.dbo.DW_MMMEDORT;

--;
    -- 1. 외래실시내역+약처방
    select
          ROW_NUMBER() over(order by newid()) AS Seq
        , *
    into Byun_origin_Rand_2M.dbo.MMEDORT_ACT
    from
      (select distinct * from
            ( select
                'MNOUTACT_MMMEDORT' as TBNM
                ,md1.PATNO as PATNO
                ,md1.PATFG as PATFG
                ,mo1.MEDDEPT as MEDDEPT
                ,mo1.ORDSEQNO as ORDSEQNO
                ,mo1.ORDCODE as ORDCODE
                ,md1.MEDDATE as MEDDATE
                ,mo1.ORDDATE as ORDDATE
                ,null as ADMDATE
                ,coalesce(mo1.ACTDTTM, mo1.CHANGEDT) as act_stdt -- acting datetime
                ,null as act_endt
                ,md1.R_DAY as R_DAY
                ,mo1.METHODCD as MEDTHODCD
                ,mo1.CNT as CNT
                ,md1.ACTCNT as ACTCNT
                ,mo1.PACKQTY as PACKQTY
                ,md1.PACKUNIT as PACKUNIT
                ,mo1.ORDDR as ORDDR
                ,md1.REMARK as REMARK
                ,md1.MKFG as MKFG
                ,md1.DRUGFG as DRUGFG
                ,md1.WARDNO as WARDNO
                ,mo1.ACTYN as ACTYN
                ,mo1.ORDDELYN as ORDDELYN
                ,md1.PRNACTYN as PRNACTYN
                ,md1.DCYN as DCYN
                ,md1.DCORDSEQ as DCORDSEQ
                ,md1.PREORDYN as PREORDYN
                ,mo1.REJTTIME as REJTTIME
            from
                (select * from Byun_origin_Rand_2M.dbo.MMMEDORT_BK) as md1
                    inner join   (select  PATNO ,PATFG,MEDDEPT ,ORDDATE,ORDSEQNO ,ORDTYP ,ORDKIND,ORDCODE
                                         ,ACTYN,ACTDTTM,ACTNEED,ORDDR,DRUGCLS,PACKQTY,CNT ,REJTTIME
                                         ,REQDATE,METHODCD,PRNACTYN,ORDDELYN,CHANGEDT
                    from Byun_origin_Rand_2M.dbo.MNOUTACT ) as mo1
                on md1.PATNO = mo1.PATNO and mo1.ORDDATE = md1.ORDDATE and mo1.ORDSEQNO = md1.ORDSEQNO and mo1.ORDCODE = md1.ORDCODE

 -- rowcount:: 1,934,890

              UNION ALL

-- 2. 병동실시내역+약처방
    select
         'MNWADACT_MMMEDORT' as TBNM
        ,dr1.PATNO as PATNO
        ,dr1.PATFG as PATFG
        ,dr1.MEDDEPT as MEDDEPT
        ,ac1.ORDSEQNO as ORDSEQNO
        ,ac1.ORDCODE as ORDCODE
        ,dr1.MEDDATE as MEDDATE
        ,ac1.ORDDATE as ORDDATE
        ,ac1.ADMTIME as ADMTIME
        ,coalesce(ac1.INDTTM,ac1.UPDTTM) as act_stdt
        ,ac1.ENDDTTM  as act_endt
        ,case
          when (ac1.ACTINGRETM is not null and ac1.ENDDTTM is not null)
                and datediff(day,ac1.INDTTM,ac1.ENDDTTM) > 0
              then datediff(day,ac1.INDTTM,ac1.ENDDTTM)+1
          else '1'
         end as R_DAY
        ,dr1.METHODCD as METHODCD
        ,ac1.CNT as CNT
        ,dr1.ACTCNT as ACTCNT
        ,dr1.PACKQTY as PACKQTY
        ,dr1.PACKUNIT as PACKUNIT
        ,dr1.ORDDR as ORDDR
        ,dr1.REMARK as REMARK
        ,dr1.MKFG as MKFG
        ,dr1.DRUGFG as DRUGFG
        ,dr1.WARDNO as WARDNO
        ,ac1.ACTFG as ACTFG
        ,null as ORDDELYN
        ,dr1.PRNACTYN as PRNACTYN
        ,dr1.DCYN as DCYN
        ,dr1.DCORDSEQ as DCORDSEQ
        ,dr1.PREORDYN as PREORDYN
        ,ac1.REJTTIME as REJTTIME

    from
         (select * from Byun_origin_Rand_2M.dbo.MMMEDORT_BK
            ) as dr1
    inner join   (select PATNO,ORDDATE,ORDSEQNO,ORDCODE,ORDCLSFG,ADMTIME
                    ,INDTTM,UPDTTM,ENDDTTM,ACTINGRETM,CNT,ACTFG,REJTTIME
             from Byun_origin_Rand_2M.dbo.MNWADACT_1
                    where ACTFG = 'Y'  ) as ac1
               on  dr1.PATNO = ac1.PATNO
          where  dr1.ORDDATE  = ac1.ORDDATE
                    AND dr1.ORDSEQNO = ac1.ORDSEQNO
                      AND dr1.ORDCODE  = ac1.ORDCODE)v)w;

                -- [2019-05-15 14:55:27] 64,918,694 rows affected in 4 m 44 s 267 ms
--
-- Step2. Acting과 중복되는 원처방 데이터 삭제 (일단 PRN ACT YN만 먼저 삭제 ) ::(MMMEDROT_BK)
--
    BEGIN TRAN
        delete from Byun_origin_Rand_2M.dbo.MMMEDORT_1
                from Byun_origin_Rand_2M.dbo.MMMEDORT_1 as m1
                 inner join (select PATNO, ORDSEQNO, ORDDATE, ORDCODE FROM Byun_origin_Rand_2M.DBO.MMEDORT_ACT WHERE ACTYN = 'Y' AND REJTTIME IS NULL AND ORDDELYN NOT IN ('Y')) AS A1
            ON A1.PATNO = M1.PATNO AND A1.ORDDATE = M1.ORDDATE AND A1.ORDSEQNO = M1.ORDSEQNO AND A1.ORDCODE = M1.ORDCODE
    COMMIT
            -- [2019-05-15 19:25:40] 1276290 rows affected in 9 m 35 s 579 ms

    -- Union with MMMEDORT_1 + Acting
        SELECT
        ROW_NUMBER() over(order by newid()) AS Seq
        , *
        INTO Byun_origin_Rand_2M.dbo.MMMEDORT_2
        FROM
          (
            select
                distinct * from
                (  select
                     'MMMEDORT' as TBNM
                    ,PATNO AS PATNO
                    ,PATFG AS PATFG
                    ,MEDDEPT AS MEDDPET
                    ,ORDSEQNO AS ORDSEQNO
                    ,ORDCODE AS ORDCODE
                    ,MEDDATE AS MEDDATE
                    ,ORDDATE AS ORDDATE
                    ,NULL AS ADMTIME
                    ,NULL AS act_stdt
                    ,NULL AS act_endt
                    ,R_DAY as R_DAY
                    ,METHODCD as METHODCD
                    ,CNT as CNT
                    ,SUBCNT as SUBCNT
                    ,ADDCNT as ADDCNT
                    ,ACTCNT as ACTCNT
                    ,PACKQTY as PACKQTY
                    ,PACKUNIT as PACKUNIT
                    ,ORDDR as ORDDR
                    ,REMARK as REMARK
                    ,MKFG as MKFG
                    ,DRUGFG as DRUGFG
                    ,WARDNO as WARDNO
                    ,null as ACTFG
                    ,null as ORDDELYN
                    ,PRNACTYN as PRNACTYN
                    ,DCYN as DCYN
                    ,DCORDSEQ as DCORDSEQ
                    ,PREORDYN as PREORDYN
                    ,null as REJTTIME

                from Byun_origin_Rand_2M.dbo.MMMEDORT_1

                union all

                select
                     TBNM
                    ,PATNO AS PATNO
                    ,PATFG AS PATFG
                    ,MEDDEPT AS MEDDPET
                    ,ORDSEQNO AS ORDSEQNO
                    ,ORDCODE AS ORDCODE
                    ,MEDDATE AS MEDDATE
                    ,ORDDATE AS ORDDATE
                    ,ADMDATE AS ADMTIME
                    ,act_stdt AS act_stdt
                    ,act_endt AS act_endt
                    ,R_DAY as R_DAY
                    ,MEDTHODCD as METHODCD
                    ,CNT as CNT
                    ,null as SUBCNT
                    ,null as ADDCNT
                    ,ACTCNT as ACTCNT
                    ,PACKQTY as PACKQTY
                    ,PACKUNIT as PACKUNIT
                    ,ORDDR as ORDDR
                    ,REMARK as REMARK
                    ,MKFG as MKFG
                    ,DRUGFG as DRUGFG
                    ,WARDNO as WARDNO
                    ,ACTYN as ACTFG
                    ,ORDDELYN as ORDDELYN
                    ,PRNACTYN as PRNACTYN
                    ,DCYN as DCYN
                    ,DCORDSEQ as DCORDSEQ
                    ,PREORDYN as PREORDYN
                    ,REJTTIME as REJTTIME

                from Byun_origin_Rand_2M.dbo.MMEDORT_ACT
                    where ACTYN='Y' AND REJTTIME IS NULL AND ORDDELYN NOT IN ('Y'))v)W;

        -- [2019-05-15 21:40:44] 113,306,335 rows affected in 7 m 55 s 124 ms

    SELECT
        *
        , ((CASE
              WHEN TBNM = 'MMMEDORT' AND (CNT IS NULL OR CNT = 0)
                     THEN 1
                   ELSE CNT END)
        + (CASE
                WHEN TBNM = 'MMMEDORT' AND SUBCNT IS NULL
                      THEN 0
                    ELSE SUBCNT END)
        - ABS((CASE
                 WHEN TBNM = 'MMMEDORT' AND ADDCNT IS NULL
                      THEN 0
                    ELSE ADDCNT END))) AS TOT_CNT
        , (case
             WHEN TBNM = 'MMMEDORT' AND DCYN = 'Y'
                 THEN 'Y'
             WHEN TBNM = 'MMMEDORT' AND DCORDSEQ IS NOT NULL
                  THEN 'Y'
             WHEN TBNM = 'MMMEDORT' AND MKFG NOT IN ('N', 'P')
                  THEN 'Y'
             WHEN TBNM = 'MMMEDORT' AND DRUGFG = 4
                  THEN 'Y'
             WHEN TBNM = 'MMMEDORT' AND PRNACTYN = 'Y'
                  THEN 'Y'
             WHEN TBNM IN ('MNWADACT_MMMEDORT', 'MNOUTACT_MMMEDORT') AND (PRNACTYN = 'Y' AND ACTFG is null)
                  THEN 'Y'
             WHEN TBNM IN ('MNWADACT_MMMEDORT', 'MNOUTACT_MMMEDORT') AND (REJTTIME IS NOT NULL OR ORDDELYN = 'Y')
                  THEN 'Y'
                    ELSE 'N'
            END) AS DEL_YN
    INTO Byun_meta_2M.dbo.Dev_Drug

     FROM Byun_origin_Rand_2M.dbo.MMMEDORT_2
                 -- [2019-05-15 23:20:09] 113,306,335 rows affected in 3 m 14 s 802 ms
select * from Byun_meta_2M.dbo.Dev_Drug;
    select count(*) from Byun_origin_Rand_2M.dbo.MMEDORT_FN where DEL_YN = 'N';
        -- 89,516,543
        -- 89,798,539
select count(*) from
(select  distinct * from
    (   select distinct  *
        , (case
             WHEN DCYN = 'Y'
                 THEN 'Y'
             WHEN  DCORDSEQ IS NOT NULL
                  THEN 'Y'
             WHEN  MKFG NOT IN ('N', 'P')
                  THEN 'Y'
             WHEN   DRUGFG = 4
                  THEN 'Y'
             WHEN  PRNACTYN = 'Y'
                  THEN 'Y'
             WHEN PREORDYN ='Y' then 'Y'
            ELSE 'N'
            END) AS DEL_YN

        from Byun_origin_Rand_2M.dbo.MMMEDORT)v
            where DEL_YN = 'N')w
    -- 90,999,502
