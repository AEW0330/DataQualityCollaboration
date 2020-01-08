/*************************************************************************/
--  Assigment: DataQueen project
--  Description: Running script for Meta Person
--  Author: Junghyun E, Byun
--  Date:  7. May, 2019
--  Job name: Random sampling Original data (EHR)
--  Language: MSSQL
--  Target data: Meta
--  ** 123409 :: 한국인 Other:: 외국인
/*************************************************************************/

-- 01. Dev_person
select
        ROW_NUMBER() over(order by newid()) as Uniq_no
        ,mp1.PATNO as PATNO
        ,mp1.SEX
        ,(case
            when
                 mb1.BIRTHDTTM is not null
            then mb1.BIRTHDTTM
            else mp1.BIRTHDAY
          end) as Btdt
        ,substring(mp1.RESNO2,1,1) as Fir_idno
        ,(case
            when substring(cast(mp1.RESNO2 as varchar),1,1) in ('1','2','3','4','9','0')
              then 'KR'
            when cast(mr1.CONT_NM as varchar) = '황인종'
              then 'Asian'
            when cast(mr1.CONT_NM as varchar) = '흑인종'
              then 'Black'
            when cast(mr1.CONT_NM as varchar) = '백인종'
              then 'White'
            when  mp1.FRNYN = 'Y'
              then 'Alien'
            else 'Alien'
           end ) as Race_GB
        ,mp1.ZIPCODE
        ,mp1.FRNYN  -- 외국인 여부
        ,(Case
            when mb1.BIRTHDTTM is not null -- 출생일시
            then 'Y'
            Else 'N'
          end) as BTime_GB
        , (case
            when cast(mp1.BIRTHDAY as date) > cast(di1.DIEDATE as date)
              then 'Y'
            when cast(mp1.BIRTHDAY as date) < cast(di1.DIEDATE as date)
              then 'N'
            else 'N'
              end ) as Death_error
        , (case
              when cast(mp1.BIRTHDAY as date) >= cast(getdate() as date) -- getdate()는 변환 기간의 End date에 맞춰 변경 가능
              then 'Y'
              Else  'N'
            end ) as Date_error
into Byun_meta_2M.dbo.Dev_person
    from (select p1.PNO,a1.* from Byun_origin_Rand_2M.dbo.ACPATBAT as a1
            join DW.dbo.PERSON_LIST as p1 on p1.PERSON_ID = a1.PATNO) as mp1
         left join  (select PTNT_NO, CONT_NM from
                        (  select
                              row_number() over(partition by PTNT_NO order by PTNT_NO, FRM_DT ) as seq
                              ,PTNT_NO
                              ,FRM_DT
                              ,CONT_NM
                                from Byun_origin_Rand_2M.dbo.Race_info_in_EMR
                                    where STUS_CD = 'Y' )v
                          where seq = '1' ) as mr1
                  on mp1.PATNO = mr1.PTNT_NO COLLATE korean_wansung_ci_as

          left join (  select
                           PATNO
                          ,ADMTIME   -- 입원일시
                          ,BIRTHDTTM  -- BIRTHOIFG
                          ,BBSEX  -- BIRTHDTTM
                          ,BIRTHOIFG -- BBSEX
                        from Byun_Meta.dbo.DW_MNBABPAT) as mb1
             on mp1.PNO = mb1.PATNO COLLATE korean_wansung_ci_as

          left join ( select
                             PATNO
                            ,DIEDATE
                      from (select row_number() over(partition by PATNO order by PATNO, DIEDATE) as seq
                                   , PATNO
                                   , DIEDATE
                              from Byun_origin_Rand_2M.dbo.Death_cerification)v
                            where seq = '1'  ) as di1
             on di1.PATNO = mp1.PATNO COLLATE korean_wansung_ci_as ;

-- [2019-05-07 16:05:08] 1,774,393 rows affected in 4 s 161 ms

  select * from DW.dbo.DW_CSCOMCDT
      where MIDGCODE = 'AC203'
          ORDER BY SMALLGCODE
select * from OMOP_VOCABULARY_2018.dbo.concept  where domain_id = 'RACE' and concept_name like 'white%';

-- 8527,White
-- 38003598,Black

select distinct Race_GB from Byun_meta_2M.dbo.Dev_person;