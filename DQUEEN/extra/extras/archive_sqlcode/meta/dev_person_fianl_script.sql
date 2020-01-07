
 select
     *
  ,case
    when date_error = 'Y' or death_error = 'Y' then 'N'
    else 'Y'
    end as include_yn
 into BYUN_META_1M.dbo.Dev_person
 from
  (select
         'ACPATBAT+MNBABPAT+Death_certification' as tbnm
        ,ROW_NUMBER() over(order by newid()) as uniq_no
        ,mp1.PATNO as patno
        ,mp1.SEX as sex
        ,(case
            when
                 mb1.BIRTHDTTM is not null
            then mb1.BIRTHDTTM
            else mp1.BIRTHDAY
          end) as Btdt
        ,substring(mp1.RESNO2,1,1) as fir_idno
        ,(case
            when substring(cast(mp1.RESNO2 as varchar),1,1) in ('1','2','3','4','9','0')
              then 'kr'
            when cast(mr1.CONT_NM as varchar) = '황인종'
              then 'mongoloid'
            when cast(mr1.CONT_NM as varchar) = '흑인종'
              then 'black'
            when cast(mr1.CONT_NM as varchar) = '백인종'
              then 'white'
            when  mp1.FRNYN = 'Y'
              then 'alien'
            else 'unknown'
           end ) as race_gb
        ,mp1.ZIPCODE as zipcode
        ,mp1.FRNYN as frnyn -- 외국인 여부
        ,(Case
            when mb1.BIRTHDTTM is not null -- 출생일시
            then 'Y'
            Else 'N'
          end) as btime_gb
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
    from (select * from BYUN_SOURCE_DATA.dbo.ACPATBAT ) as mp1
         left join  (select PTNT_NO, CONT_NM from
                        (  select
                              row_number() over(partition by PTNT_NO order by PTNT_NO, FRM_DT ) as seq
                              ,PTNT_NO
                              ,FRM_DT
                              ,CONT_NM
                                from BYUN_SOURCE_DATA.dbo.Race_info_in_EMR
                                    where STUS_CD = 'Y' )v
                          where seq = '1' ) as mr1
                  on mp1.PATNO = mr1.PTNT_NO

          left join (  select * from
                        (select
                          row_number() over(partition by PATNO order by PATNO,ADMTIME) as seq
                          , PATNO
                          ,ADMTIME   -- 입원일시
                          ,BIRTHDTTM  -- BIRTHOIFG
                          ,BBSEX  -- BIRTHDTTM
                          ,BIRTHOIFG -- BBSEX
                        from BYUN_SOURCE_DATA.dbo.MNBABPAT)v
                        where seq = 1) as mb1
             on mp1.PATNO = mb1.PATNO

          left join ( select
                             PATNO
                            ,DIEDATE
                      from (select row_number() over(partition by PATNO order by PATNO, DIEDATE) as seq
                                   , PATNO
                                   , DIEDATE
                              from BYUN_SOURCE_DATA.dbo.Death_cerification)v
                            where seq = '1'  ) as di1
             on di1.PATNO = mp1.PATNO)v

 --> [2019-10-28 15:36:23] 100,000 rows affected in 1 s 140 ms (중복 이슈 없음)

