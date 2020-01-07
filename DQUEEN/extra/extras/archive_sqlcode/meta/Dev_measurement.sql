-- MEASUREMENT_TYPE_CONCEPT_ID
-- OPERATOR_CONCEPT_ID
-- VALUE_AS_NUMBER
-- VALUE_AS_CONCEPT_ID
-- VALUE_SOURCE_VALUE

    SELECT
         M1.*
        ,case
            when M1.MEDDATE <= dp1.Btdt then 'Y'
            when M1.ORDDATE is not null and M1.ORDDATE <= dp1.Btdt then 'Y'
            when M1.ACPTTIME is not null and M1.ACPTTIME < dp1.Btdt then 'Y'
            when M1.EXECTIME is not null and M1.EXECTIME < dp1.Btdt then 'Y'
            when M1.VERIFYTM is not null and M1.VERIFYTM < dp1.Btdt then 'Y'
            else 'N'
         end as Brith_err
        ,case
            when M1.MEDDATE > dd1.DIEDATE then 'Y'
            when M1.ORDDATE is not null and M1.ORDDATE > dd1.DIEDATE then 'Y'
            when M1.ACPTTIME is not null and M1.ACPTTIME > dd1.DIEDATE then 'Y'
            when M1.EXECTIME is not null and M1.EXECTIME > dd1.DIEDATE then 'Y'
            when M1.VERIFYTM is not null and M1.VERIFYTM < dp1.Btdt then 'Y'
            else 'N'
         end as death_err
        ,case
            when M1.MEDDATE > M1.ACPTTIME or M1.MEDDATE > M1.EXECTIME or M1.MEDDATE > M1.VERIFYTM  then 'Y'
            when M1.ORDDATE > M1.ACPTTIME or M1.ORDDATE > M1.EXECTIME or M1.ORDDATE > M1.VERIFYTM or M1.ORDDATE > M1.MEDDATE then 'Y'
            when M1.ACPTTIME > M1.EXECTIME or M1.ACPTTIME > M1.VERIFYTM then 'Y'
            when M1.EXECTIME > M1.VERIFYTM then 'Y'
            else 'N'
          end as date_err
    into Byun_Meta_2M.dbo.Dev_measurment1
    FROM
      (select
           TABLENM
          ,UID
          ,PATNO
          ,MED_DEPT
          ,PATFG
          ,case
            when ADMTIME is not null then ADMTIME
            else MEDDATE
           end as MEDDATE
          ,ORDDATE
          ,ACPTTIME
          ,EXECTIME
          ,VERIFYTM
          ,case
            when EXECTIME is not null then cast(EXECTIME as date)
            when RECDATETIME is not null then cast(RECDATETIME as date)
            else cast(COALESCE(ADMTIME,MEDDATE) as date)
           end as MEASUREMENT_DT
         ,case
            when EXECTIME is not null then cast(EXECTIME as datetime)
            when RECDATETIME is not null then cast(RECDATETIME as datetime)
            else cast(COALESCE(ADMTIME,MEDDATE) as datetime)
           end as MEASUREMENT_DTTM
         ,EXAMTYP as typ
         ,ORDSEQNO
         ,EXAMCODE
         ,ORDCODE
         ,COALESCE(RSLTTYPE,INS_TYPE) as val_type
         ,RSLTNUM
         ,RSLTTEXT
         ,UNIT
         ,NORMMAXVAL
         ,NORMMINVAL
         ,PROVIDER_ID
         ,PRNACTYN
         ,DCYN
         ,MKFG
         ,VLD_GB
         , CASE WHEN DCYN = 'Y'  THEN  'Y'
             WHEN MKFG NOT IN ('N', 'P')  THEN  'Y'
             WHEN PRNACTYN = 'Y'  THEN  'Y'
             WHEN VLD_GB not in ('Y')  THEN  'Y'
             ELSE 'N'
            END AS Del_YN

        from
         Byun_meta_2M.dbo.meta_measurement) AS M1
   inner join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as dp1
                on dp1.PATNO = M1.PATNO COLLATE Korean_Wansung_CI_AS
   left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as dd1
                on dd1.PATNO = M1.PATNO COLLATE Korean_Wansung_CI_AS
; SELECT * FROM Byun_Meta_2M.DBO.meta_measurement;

;


