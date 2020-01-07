/*************************************************************************/
--  Assigment: DataQueen project
--  Description: Running script for Meta Person
--  Author: Junghyun E, Byun
--  Date:  7. May, 2019
--  Job name: Random sampling Original data (EHR)
--  Language: MSSQL
--  Target data: Meta

--  B.DRST in ('1','2','3','4','5') THEN  4010577  --(Medical doctor)
--  WHEN B.DRST ='6' THEN  4010474 --(Practice nurse)
--	ELSE 38004698	 -- (All Other Suppliers)
/*************************************************************************/

-- Step 1.
        select
             ROW_NUMBER() over(order by newid()) AS uniq_num
            ,row_number()over(partition by patno, substring(cast(visit_date as varchar),1,10) order by patno, visit_date,visit_time) as p_seq

            ,*
        into Byun_meta_2M.dbo.Dev_visit
        from
            (
            -- 입원/통원/응급 내역
              -- 진료.퇴원결과(I:AI009/E:MN238) 1,계속::2,이송::3,회송::4,사망::9,퇴원

                 select
                       'APIPDSLT' as tbnm
                      ,ap1.PATNO as patno
                      ,ap1.PATFG as patfg
                      ,ap1.OIFG  as oifg
                      ,ap1.ADMTIME  as visit_date
                      ,ap1.ADMTIME  as visit_time
                      ,ap1.ERTIME as ERTIME
                      ,sd1.DSCHDATE as dsch_date
                      ,ap1.DSCHTIME as dsch_time
                      ,ap1.MEDDAY as med_day
                      ,ap1.MEDDEPT as med_dept
                      ,sd1.DSCHDEPT as dsch_dept
                      ,ap1.CHADR as cha_dr
                      ,ap1.GENDR as gen_dr
                      ,sd1.ERCHADR as er_dr
                      ,ap1.DSCHORDR as dschor_dr
                      ,case
                         when ap1.ADMPATH = '1' then 'OP' -- Out patients
                         when ap1.ADMPATH = '2' then 'ER'
                         when ap1.ADMPATH = '3' then 'DR' --분만실
                         when ap1.ADMPATH = '4' then 'NB' -- New born
                         when ap1.ADMPATH = '5' then 'OR'
                         when ap1.ADMPATH = '6' then 'RD' -- Reservation after discharge
                         else ap1.ADMPATH
                           end   as ADMPATH
                      ,case
                         when ap1.DSCHRSLT = '1' then 'Continue'
                         when ap1.DSCHRSLT = '2' then 'Transfer'
                         when ap1.DSCHRSLT = '3' then 'Return'
                         when ap1.DSCHRSLT = '4' then 'Death'
                         when ap1.DSCHRSLT = '9' then 'Discharge'
                         else ap1.DSCHRSLT
                           end   as DSCHRSLT
                      ,case
                         when sd1.DSCHTYP = '1' then 'OR'    --지시 후
                         when sd1.DSCHTYP = '2' then 'Self'  --자의
                         when sd1.DSCHTYP = '3' then 'Away'    --탈원
                         when sd1.DSCHTYP = '4' then 'Transfer'    --전원
                         when sd1.DSCHTYP = '5' then 'Death'    --사망
                         when sd1.DSCHTYP = '6' then 'ETC'    --기타
                         else sd1.DSCHTYP
                           end   as DSCHTYP
                      ,ap1.WARDNO
                      ,sd1.DSCHWARD
                      ,sd1.MAINAPPL
                      ,sd1.DDRSNCD
                      ,(case
                          when ap1.FSTMEDTYP = '1' then '초진'
                          when ap1.FSTMEDTYP = '2' then '재진'
                          when ap1.FSTMEDTYP = '3' then '신환'
                          else ap1.FSTMEDTYP
                           end)  as FSTMEDTYP
                      ,ap1.REJTTIME
                      ,ap1.REJTCD
                      ,case
                         when ap1.DSCHTIME > ap1.ADMTIME then 'N'
                         else 'Y'
                           end   as Date_error
                      ,case
                          when ap1.DSCHTIME > dd1.DIEDATE or ap1.ADMTIME > dd1.DIEDATE then 'Y'
                          else 'N'
                        end as Death_error
                      ,case
                          when ap1.DSCHTIME < p1.Btdt or ap1.ADMTIME < p1.Btdt then 'Y'
                          else 'N'
                        end as Birth_error
                      ,case
                          when ap1.REJTTIME is not null then 'N'
                          else 'Y'
                        end as include_yn

                from (select * from Byun_origin_Rand_2M.dbo.APIPDLST) as ap1
                       left join (select * from Byun_origin_Rand_2M.dbo.SMIDSCHT) as sd1
                         on ap1.PATNO = sd1.PATNO and ap1.ADMTIME = sd1.ADMTIME
                        left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as dd1
                          on dd1.PATNO = ap1.PATNO
                        join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as p1
                          on p1.PATNO = ap1.PATNO
            union all
            -- 외래

            select
                     'AOOPDLST' as tbnm
                    ,ad1.PATNO	as ptno , --환자ID
                    'O' as patfg
                    ,ad1.OIFG as oifg --외래/입원 구분
                    ,ad1.MEDDATE as visit_date	 --진료일자
                    ,ad1.MEDTIME as visit_time --진료일시
                    ,null as ERTIME
                    ,ad1.MEDDATE as dsch_date
                    ,null as dsch_datetime
                    ,0 as med_day
                    ,ad1.MEDDEPT as med_dept	, --진료과
                    ad1.MEDDEPT as dsch_dept
                    ,ad1.MEDDR	as char_dr, --진료의사
                    null as gen_dr
                    ,null as er_dr
                    ,null as dschor_dr
                    ,null as admpath
                    ,ad1.OTHOSPMEDYN as dschrslt
                    ,null as dschtyp
                    ,null as wardno
                    ,null as dschward
                    ,null as mainppl
                    ,null as ddrsncd
                    ,case   --초재진구분(AP019) 1,초진::2,재진::3,신환 FSTMEDTYP
                        when ad1.FSTMEDTYP is not null then (select c1.CODENAME from DW.dbo.DW_CSCOMCDT as c1
                                                        where c1.MIDGCODE='AP019' and c1.SMALLGCODE = ad1.FSTMEDTYP)
                        else null
                      end as fstmedtyp
                    ,ad1.REJTTIME, --취소일시
                    ad1.REJTCD, --취소사유(AP032) 2,주치의변경::3,진료거부::4,작업오류::5,입원::6,중복처방::7,검사실패::8,기타
                     case
                        when ad1.MEDTIME is not null and cast(ad1.MEDTIME as date) = cast(MEDDATE as date) then 'N'
                        else 'Y'
                    end as Date_error
                    ,case
                        when ad1.MEDDATE > d1.DIEDATE then 'Y'
                        else 'N'
                      end as death_error
                    ,case
                        when ad1.MEDDATE < d1.Btdt then 'Y'
                        else 'N'
                      end as birth_error
                    ,case
                        when ad1.REJTTIME is null then 'Y'
                        else 'N'
                    end as Include_yn
                from Byun_origin_Rand_2M.dbo.AOOPDLST as ad1
                  left join (select dc1.PATNO, dc1.DIEDATE, ap1.Btdt from Byun_origin_Rand_2M.dbo.Death_cerification as dc1
                                join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as ap1
                                    on ap1.PATNO = dc1.PATNO) as d1
                              on d1.PATNO = ad1.PATNO

            union all
            --검진

               select
                   'MPRECEIT' as tbnm
                  ,mc1.PATNO as patno	  , -- 환자ID
                  'G' as PATFG
                  ,null as oifg
                  ,mc1.ODT as visit_date   , -- 검진일자
                  (substring(cast(mc1.ODT as varchar),1,10)+' '+
                   (case
                      when mc1.BTIME is not null and len(mc1.BTIME) = 6 then substring(mc1.BTIME,1,2)+':'+substring(mc1.BTIME,3,2)+':'+substring(mc1.BTIME,5,2)
                      when mc1.BTIME is not null and len(mc1.BTIME) = 5 then mc1.BTIME
                      when mc1.BTIME is not null and len(mc1.BTIME) = 4 then substring(mc1.BTIME,1,2)+':'+substring(mc1.BTIME,3,2)
                      else null
                   end)) as visit_time
                  ,null as ERTIME
                  ,mc1.odt as dsch_date
                  ,(substring(cast(mc1.ODT as varchar),1,10)+' '+
                    (case
                      when mc1.ETIME is not null and len(mc1.ETIME) = 6 then substring(mc1.ETIME,1,2)+':'+substring(mc1.ETIME,3,2)+':'+substring(mc1.ETIME,5,2)
                      when mc1.ETIME is not null and len(mc1.ETIME) = 5 then mc1.ETIME
                      when mc1.ETIME is not null and len(mc1.ETIME) = 4 then substring(mc1.ETIME,1,2)+':'+substring(mc1.ETIME,3,2)
                      else null
                   end)) as dsch_time
                  ,0 as medday
                  ,null as meddept
                  ,null as dsch_dept
                  ,mc1.FANDR as char_dr , -- 판독의 ID
                  null as gen_dr
                  ,null as er_dr
                  ,null as dsch_dr
                  ,null as admpath
                  ,null as dschrlst
                  ,null as dschtyup
                  ,null as wardno
                  ,null as dschward
                  ,null as mainppl
                  ,null as ddrsncd
                  ,null as fstmedtyp
                  ,mc1.CANDT as rejttime , -- 검진취소일자
                  mc1.CANCD as rejtcd	  , -- 검진취소사유코드
                  'N' as date_error
                  ,case
                      when mc1.ODT > er1.DIEDATE then 'Y'
                      else 'N'
                    end as death_error
                  ,case
                      when mc1.ODT < er1.Btdt  then 'Y'
                      else 'N'
                    end as birth_error
                  ,case
                      when mc1.CANDT is not null or mc1.CANCD is not null or mc1.CANRMK is not null then 'N'
                      when (mc1.CANDT is not null and FANDT is not null) then 'n'
                      else 'Y'
                    end as Include_yn

                  from (select * from Byun_origin_Rand_2M.dbo.MPRECEIT) as mc1
                  left join (select dc1.PATNO, dc1.DIEDATE, ap1.Btdt from Byun_origin_Rand_2M.dbo.Death_cerification as dc1
                                join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as ap1
                                    on ap1.PATNO = dc1.PATNO) as er1
                  on er1.PATNO = mc1.PATNO

                union all
              -- 산업의학


                select
                    'MIRECEIT' as tbnm
                    ,mc1.PATNO as patno	, --환자ID
                    'S' as patfg
                    ,'S' as oifg
                    ,mc1.ODT as visit_date	, --검진일자
                    substring(cast(mc1.ODT as varchar),1,10)+' '+
                     (case
                       when mc1.BTIME is not null and len(mc1.BTIME) = 6 then substring(mc1.BTIME,1,2)+':'+substring(mc1.BTIME,3,2)+':'+substring(mc1.BTIME,5,2)
                       when mc1.BTIME is not null and len(mc1.BTIME) = 5 then mc1.BTIME
                       when mc1.BTIME is not null and len(mc1.BTIME) = 4 then substring(mc1.BTIME,1,2)+':'+substring(mc1.BTIME,3,2)
                       else null
                     end) as visit_time
                    ,null as ERTIME
                    ,mc1.ODT as dsch_date
                    ,substring(cast(mc1.ODT as varchar),1,10)+' '+
                     (case
                       when mc1.ETIME is not null and len(mc1.ETIME) = 6 then substring(mc1.ETIME,1,2)+':'+substring(mc1.ETIME,3,2)+':'+substring(mc1.ETIME,5,2)
                       when mc1.ETIME is not null and len(mc1.ETIME) = 5 then mc1.ETIME
                       when mc1.ETIME is not null and len(mc1.ETIME) = 4 then substring(mc1.ETIME,1,2)+':'+substring(mc1.ETIME,3,2)
                       else null
                     end)  as dsch_time
                    ,0 as med_day
                    ,null as med_dept
                    ,null as dsch_dept
                    ,mc1.FANDR as char_dr , --판정의사
                    null as gen_Dr
                    ,null as er_dr
                    ,null as dschor_dr
                    ,null as admpath
                    ,null as dschrslt
                    ,null as dschtyp
                    ,null as wardno
                    ,null as dschward
                    ,null as mainppl
                    ,null as ddrsncd
                    ,null as fstmedtyp
                    ,mc1.CANDT as rejttime	, --검진취소일자
                    null as rejtcd
                    ,'N' as date_error
                    ,case
                        when mc1.ODT > er1.DIEDATE then 'Y'
                        else 'N'
                      end as death_error
                    ,case
                        when mc1.ODT < er1.Btdt then 'Y'
                        else 'N'
                      end as birth_error
                    ,case
                      when mc1.CANDT is not null or mc1.CANDT is not null then 'N'
                      when (mc1.CANDT is not null and mc1.VISITFG = 'N') then 'N'
                      else 'Y'
                     end as Include_yn
                  from Byun_origin_Rand_2M.dbo.MIRECEIT as mc1
                  left join (select dc1.PATNO, dc1.DIEDATE, ap1.Btdt from Byun_origin_Rand_2M.dbo.Death_cerification as dc1
                       join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as ap1
                             on ap1.PATNO = dc1.PATNO) as er1
                   on er1.PATNO = mc1.PATNO) v;

      -- [2019-05-09 21:58:06] 37950128 rows affected in 5 m 31 s 665 ms

-- Step 2


        select
         /*  case when ERTIME is not null and patfg = 'I' and p_seq ='1'
                then (select distinct uniq_num from Byun_meta_2M.dbo.Dev_visit where (c.ERTIME = visit_date)and (patfg ='E' and p_seq ='1') )
                when ERTIME is null and patfg in ('I','E') and p_seq = '1'
                then uniq_num
                when patfg in ('O','S','G') and p_seq ='1' then uniq_num
                else null
            end as visit_id */
          lag(uniq_num) over(partition by patno order by patno, visit_date,visit_time) as detail_preceding
          ,*
        into Byun_meta_2M.dbo.Dev_visit_fn
        from

           (select * from Byun_meta_2M.dbo.Dev_visit
              where include_yn = 'Y' )c

-- Step 3

    select
     n2.uniq_num as ei_visit
    , n1.*
    -- into #aa
    from (select * from Byun_meta_2M.dbo.Dev_visit_fn where  patfg in ('E','I'))as n1
      left join (select uniq_num, patno, visit_date, ERTIME from Byun_meta_2M.dbo.Dev_visit_fn
                    where patfg = 'E' and p_seq ='1' and ERTIME is not null ) as n2
        on n2.patno = n1.patno and n2.ERTIME = n1.visit_date

--

---------------------------
select * into Byun_meta_2M.dbo.Dev_visit_detail from
(
select
       v2.uniq_num as ei_gb
       ,v1.*

from Byun_meta_2M.dbo.Dev_visit_fn as v1
  left join (select uniq_num, patno, visit_time,patfg,p_seq from Byun_meta_2M.dbo.Dev_visit_fn where patfg = 'I' and p_seq ='1') as v2
    on v1.patno = v2.patno and v1.ERTIME =v2.visit_time)v

;
-- [2019-07-17 01:13:57] 31591353 rows affected in 8 s 537 ms

select * from  Byun_meta_2M.dbo.Dev_visit_detail where ei_gb is not null ;