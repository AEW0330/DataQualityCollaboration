/*************************************************************************/
--  Assigment: DataQueen project
--  Description: Running script for Meta Person
--  Author: Junghyun E, Byun
--  Date:  22. May, 2019
--  Job name: Random sampling Original data (EHR)
--  Language: MSSQL
--  Target data: Meta
--  Description
/*************************************************************************/
select
    ROW_NUMBER() over(order by newid()) AS uniq_num
  ,'MMTRTORT_D2' as TBNM
  ,de1.PATNO
  ,de1.MEDDEPT
  ,de1.PATFG
  ,de1.MEDDATE
  ,de1.ORDDATE
  ,de1.DEVICE_STDT
  ,de1.DEVICE_STDT_TM
  ,de1.DEVICE_ENDT
  ,de1.DEVICE_ENDT_TM
  ,de1.ORDSEQNO
  ,de1.ORDCODE
  ,de1.ORDDR
  ,de1.CNT
  ,de1.Day
  ,(de1.CNT*de1.Day) as Qty
  ,de1.WARDNO
  ,de1.PRNACTYN
  ,de1.MKFG
  ,DCYN
  ,DCORDSEQ
  ,REGTIME
  ,de1.Del_yn
  ,case
            when de1.MEDDATE <= dp1.Btdt then 'Y'
            when de1.ORDDATE is not null and de1.ORDDATE <= dp1.Btdt then 'Y'
            when de1.EXECPLDT is not null and de1.EXECPLDT < dp1.Btdt then 'Y'
            when de1.REGTIME is not null and de1.REGTIME < dp1.Btdt then 'Y'
            else 'N'
         end as Brith_err
   ,case
            when de1.MEDDATE > dd1.DIEDATE then 'Y'
            when de1.ORDDATE is not null and de1.ORDDATE > dd1.DIEDATE then 'Y'
            when de1.EXECPLDT is not null and de1.EXECPLDT > dd1.DIEDATE then 'Y'
            when de1.REGTIME is not null and de1.REGTIME > dd1.DIEDATE then 'Y'
            else 'N'
         end as death_err

   ,case
            when de1.MEDDATE > de1.ORDDATE then 'Y'
            when de1.ORDDATE > de1.EXECPLDT or de1.ORDDATE > de1.OPDATE then 'Y'
            when de1.MEDDATE > de1.OPDATE  then 'Y'
            else 'N'
     end as date_err
-- into Byun_meta_2M.dbo.Dev_Device

    from
    (select
      v.*
      ,case
          when v.DEVICE_STDT is not null then dateadd(day,v.Day,v.DEVICE_STDT)
          else null
       end as DEVICE_ENDT
      ,case
          when v.DEVICE_STDT_TM is not null then dateadd(day,v.Day,v.DEVICE_STDT_TM)
          else null
       end as DEVICE_ENDT_TM
 from
    (select
         a.PATNO  -- 환자ID
        ,a.PATFG   -- 내원구분
        ,a.MEDDATE   -- 진료/입원/수술(DSC)/응급실도착일시
        ,a.ORDDATE   -- 처방일자
        ,a.OPDATE
        ,case
          when a.ANETHSTM is not null then cast(a.ANETHSTM as date)
          when a.OPDATE is not null then cast(a.OPDATE as date)
          else a.ORDDATE
         end as DEVICE_STDT
        ,case
          when a.ANETHSTM is not null then a.ANETHSTM
          else null
         end as DEVICE_STDT_TM
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
                        from Byun_origin_Rand_2M.dbo.MMTRTORT_Device ) as a
    )as v

    )as de1
   inner join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as dp1
                on dp1.PATNO = de1.PATNO
   left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as dd1
                on dd1.PATNO = de1.PATNO;

