--> step 1
select
         p1.tbnm as tbnm
        ,ROW_NUMBER() over(order by newid()) AS uniq_no
        ,p1.ORDSEQNO as ordseqno
        ,p1.SEQNO as other_seqno
        ,p1.seq as exec_seq
        ,p1.patno
        ,p1.MEDDEPT as meddept
        ,p1.patfg
        ,p1.MEDDATE as meddate
        ,p1.orddate as orddate
        ,p1.procedure_date as procedure_date
        ,p1.procedure_datetime as procedure_datetime
        ,p1.ORDCODE as ordcode
        ,p1.cnt
        ,p1.actcnt
        ,p1.R_DAY as day
        ,p1.typ  as ordtyp
        ,p1.MKFG as ord_gb
        ,p1.ORDDR as orddr
        ,p1.WARDNO as wardno
      --,v1.visit_id
        ,p1.DCYN as dc_yn
        ,null as prn_yn
        ,p1.PRNACTYN as prnact_yn
        ,p1.exc_yn
                ,case
            when cast(p1.MEDDATE as datetime2) > cast(dd1.death_dt as datetime2) then 'Y'
            when p1.ORDDATE is not null and cast(p1.ORDDATE as datetime2) > cast(dd1.death_dt as datetime2) then 'Y'
            when p1.procedure_date is not null and cast(p1.procedure_date as datetime2) > cast(dd1.death_dt as datetime2) then 'Y'
            when p1.procedure_datetime is not null and cast(p1.procedure_datetime as datetime2) > cast(dd1.death_dt as datetime2) then 'Y'
             when p1.REGTIME is not null and cast(p1.REGTIME as datetime2) > cast(dd1.death_dt as datetime2) then 'Y'
            else 'N'
         end as death_err
        ,case
            when cast(p1.MEDDATE as datetime2) <= cast(dp1.Btdt as datetime2) then 'Y'
            when cast(p1.ORDDATE as datetime2) is not null and cast(p1.ORDDATE as datetime2) <= cast(dp1.Btdt as datetime2) then 'Y'
            when p1.procedure_date is not null and cast(p1.procedure_date as datetime2) <= cast(dp1.Btdt as datetime2) then 'Y'
            when p1.procedure_datetime is not null and cast(p1.procedure_datetime as datetime2) <= cast(dp1.Btdt as datetime2) then 'Y'
            when p1.REGTIME is not null and cast(p1.REGTIME as datetime2) < cast(dp1.Btdt as datetime2) then 'Y'
            else 'N'
         end as Brith_err
        ,case
            when cast(p1.MEDDATE as datetime2) > cast(p1.ORDDATE as datetime2) then 'Y'
            when cast(p1.ORDDATE as datetime2) > cast(p1.procedure_date as datetime2) or cast(p1.ORDDATE as datetime2) > cast(p1.procedure_datetime as datetime)  then 'Y'
            when cast(p1.MEDDATE as datetime2) > cast(p1.REGTIME as datetime2) then 'Y'
          else 'N'
        end as date_err
into Byun_meta_1M.dbo.Dev_procedure_step1
from
(
--입원 응급 통원수술
  select
        m1.*
  from
    (select

          'MMTRTORT' as tbnm
        , m1.PATNO
        , m1.MEDDEPT
        , m1.PATFG
        , m1.SEQNO
        , m1.ORDSEQNO -- 처방순번
        , m2.EXECSEQ as seq
        , m1.MEDDATE
        , m1.ORDDATE
        , case
            when m2.ACTINGRETM is not null then cast(m2.ACTINGRETM as date)
            when m2.ACTINGRETM is not null then cast(m2.INDTTM as date)
            when (m2.ACTINGRETM is null and m2.INDTTM is null) and m1.opdate is not null then cast(m1.OPDATE as date)
            when (m2.ACTINGRETM is null and m2.INDTTM is null and m1.opdate is null) and m1.ORDDATE is not null then cast(m1.ORDDATE as date)
          ELSE cast(m1.MEDDATE as date)
          end as Procedure_date
        , case
            when m2.ACTINGRETM is not null then ACTINGRETM
            when m2.ACTINGRETM is null then m2.INDTTM
            else null
          end as procedure_datetime
        , m2.INDTTM
        , m1.ORDKIND as typ
        , m1.ORDCODE
        , m1.ORDNAME
        , case
            when m2.CNT is null or m2.CNT = '0' then 1
            ELSE m2.CNT
          end as ACTCNT
        , m1.CNT
        , m1.RETNQTY
        , m1.R_DAY
        , m1.ORDDR -- 처방의사
        , m1.CHADR
        , m1.PRNACTYN
        , m1.DCYN
        , m1.DCORDSEQ
        , m1.MKFG
        , m1.REGTIME
        , m1.WARDNO
        , m2.REJTTIME
        , case
            when m1.DCYN = 'Y' then 'N'
            when m1.DCORDSEQ is not null then 'N'
            when m1.MKFG not in ('N','P') then 'N'
            when m2.REJTTIME is not null then 'N'
           -- when m1.PRNACTYN = 'Y' then 'Y'
          else 'Y'
          end as exc_yn
   from
     (select *  from BYUN_SOURCE_DATA.dbo.MMTRTORT
      WHERE ORDCLSTYP IN ('D1','D3','D4') and PATFG in ('I','E','D')) as m1
      left join (select PATNO,ORDDATE,ORDSEQNO,EXECSEQ,ORDCODE,ORDCLSFG,ADMTIME
                       ,INDTTM,UPDTTM,ENDDTTM,ACTINGRETM,CNT,ACTFG,REJTTIME,(ROW_NUMBER() over(order by newid()))+100 AS Seq
                from BYUN_SOURCE_DATA.dbo.MNWADACT where ACTFG= 'Y') as m2
    on m1.PATNO = m2.PATNO
    where m1.ORDDATE = m2.ORDDATE and m1.ORDSEQNO = m2.ORDSEQNO and m1.ORDCODE = m2.ORDCODE

    union all
-- 외래
       select
        'MMTRTORT' as tbnm
        ,  m1.PATNO
        , m1.MEDDEPT
        , m1.PATFG
        , m1.SEQNO
        , m1.ORDSEQNO -- 처방순번
        ,null as Seq
        , m1.MEDDATE
        , m1.ORDDATE
        , case
            when m1.OPDATE is not null then cast(m1.OPDATE as date)
            when m1.OPDATE is null then cast(m1.ORDDATE as date)
          ELSE cast(m1.MEDDATE as date)
          end as Procedure_date
        , case when m1.ANETHSTM is null then m1.ANETHSTM
          else null
          end as procedure_datetime
        , null as INDTTM
        , m1.ORDKIND as typ
        , m1.ORDCODE
        , m1.ORDNAME
        , case
            when m1.ACTCNT is null or m1.ACTCNT = '0' then 1
            ELSE m1.ACTCNT
          end as ACTCNT
        , m1.CNT
        , m1.RETNQTY
        , m1.R_DAY
        , m1.ORDDR -- 처방의사
        , m1.CHADR
        , m1.PRNACTYN
        , m1.DCYN
        , m1.DCORDSEQ
        , m1.MKFG
        , m1.REGTIME
        , m1.WARDNO
        , null as REJTTIME
        , case
            when m1.DCYN = 'Y' then 'N'
            when m1.DCORDSEQ is not null then 'N'
            when m1.MKFG not in ('N','P') then 'N'
            when m1.PRNACTYN = 'Y' then 'N'
          else 'Y'
          end as exc_yn
   from
     (select *  from BYUN_SOURCE_DATA.dbo.MMTRTORT
      WHERE ORDCLSTYP IN ('D1','D3','D4') and PATFG in ('O','H','G','M')) as m1 )as m1
union all
-- 2.MMEXMORT 검사(검체검사제외)

  select
         'MMEXMORT' as tbnm
        ,e1.PATNO  --환자ID
        ,e1.MEDDEPT  --,진료과
        ,e1.PATFG  --내원구분
        ,null as SEQNO
        ,e1.ORDSEQNO --처방순번
        ,e2.EXECSEQ as seq
        ,e1.MEDDATE  -- 진료/입원/수술(DSC)/응급실도착일시
        ,e1.ORDDATE  -- 처방일자
        ,case
          when e1.EXECTIME is not null then cast(e1.EXECTIME as date)
          when e1.EXECTIME is null and e2.INDTTM is not null then cast(e2.INDTTM as date)
          when e1.EXECTIME is null and e2.INDTTM is null and e2.ACTINGRETM is not null then cast(e2.ACTINGRETM as date)
        else e1.ORDDATE
        end as  procedure_date
        ,case
          when e1.EXECTIME is not null then cast(e1.EXECTIME as datetime)
          when e1.EXECTIME is null and e2.INDTTM is not null then cast(e2.INDTTM as datetime)
          when e1.EXECTIME is null and e2.INDTTM is null and e2.ACTINGRETM is not null then cast(e2.ACTINGRETM as datetime)
        else e1.ORDDATE
        end as procedure_datetime
        ,e2.INDTTM
        ,e1.EXAMTYP as typ  --,검사분류
        ,e1.ORDCODE  --,처방코드
        ,null as ORDNAME
        ,case
           when e2.CNT is null or e2.CNT = '0' then  1
           else e2.CNT
          end as ACTCNT
        ,case
            when e1.CNT is null or e1.CNT = '0' then  1
            else e1.CNT
          end as CNT
        ,'1' as R_DAY
        ,null as RETNQTY
        ,e1.ORDDR  --,처방의사
        ,e1.CHADR  --,주치의
        ,e1.PRNACTYN --,PRN실시처방여부
        ,e1.DCYN --,D/C여부
        ,e1.DCORDSEQ --,D/C원처방번호
        ,e1.MKFG --,처방발생구분(MM003)
        ,e1.REGTIME
        ,e1.WARDNO --,병동
        ,e2.REJTTIME
        ,case
            when e1.DCYN = 'Y' then 'N'
            when e1.DCORDSEQ is not null then 'N'
            when e1.MKFG not in ('N','P') then 'N'
            when e2.REJTTIME is not null then 'N'
            when e1.EXAMRTNYN = 'Y' then 'N'--,검사반환여부
          else 'Y'
          end as exc_yn
    from (select PATNO,ORDSEQNO,ORDDATE,MEDDATE,OPDATE,ACPTTIME,EXECTIME,PATFG,ORDCLSTYP,ORDCODE
                ,MEDDEPT,ORDDR,CHADR,EXECDR,WARDNO,QTY,CNT,EXAMTYP,PRNACTYN,DCYN,DCORDSEQ,MKFG,EXAMRTNYN,REGTIME
                                       from Byun_origin_Rand_2M.dbo.MMEXMORT where PATFG in ('I','E','D') and ORDCLSTYP IN ('C2','C3')) as e1
    left join (select PATNO,ORDDATE,ORDSEQNO,ORDCODE,ORDCLSFG,ADMTIME,EXECSEQ,ACTINGRETM
                       ,INDTTM,CNT,ACTFG,REJTTIME,(ROW_NUMBER() over(order by newid()))+1000 AS Seq from Byun_origin_Rand_2M.dbo.MNWADACT where ACTFG= 'Y') as e2
      on e1.PATNO = e2.PATNO and e1.ORDDATE = e2.ORDDATE and e1.ORDSEQNO = e2.ORDSEQNO and e1.ORDCODE = e2.ORDCODE


union all

       select
        'MMEXMORT' as tbnm
        ,e1.PATNO  --,환자ID
        ,e1.MEDDEPT  --,진료과
        ,e1.PATFG
        ,null as SEQNO
        ,e1.ORDSEQNO --,처방순번
        ,null as Seq
        ,e1.MEDDATE  --,진료/입원/수술(DSC)/응급실도착일시
        ,e1.ORDDATE  --,처방일자
        ,case
          when e1.EXECTIME is not null then cast(e1.EXECTIME as date)
        else e1.ORDDATE
        end as  procedure_date
        ,case
          when e1.EXECTIME is not null then cast(e1.EXECTIME as datetime2)
        else e1.ORDDATE
        end as  procedure_datetime
        ,e1.EXECTIME as INDTTM --,실시일시
        ,e1.EXAMTYP as TYP
        ,e1.ORDCODE  --,처방코드
        ,null as ORDNAME
        ,null as ACTCNT
        ,case
            when e1.CNT is null or e1.CNT = '0' then  1
            else e1.CNT
          end as CNT
        ,'1' as R_DAY
        ,null as RETNQTY
        ,e1.ORDDR  --,처방의사
        ,e1.CHADR  --,주치의
        ,e1.PRNACTYN --,PRN실시처방여부
        ,e1.DCYN --,D/C여부
        ,e1.DCORDSEQ --,D/C원처방번호
        ,e1.MKFG --,처방발생구분(MM003)
        ,e1.REGTIME
        ,e1.WARDNO --,병동
        ,null as REJTTIME
        ,case
            when e1.DCYN = 'Y' then 'N'
            when e1.DCORDSEQ is not null then 'N'
            when e1.MKFG not in ('N','P') then 'N'
            when e1.EXAMRTNYN = 'Y' then 'N'
           -- when e2.REJTTIME is not null then 'Y'
            when e1.PRNACTYN = 'Y' then 'N'
          else 'Y'
          end as exc_yn
    from (select PATNO,ORDSEQNO,ORDDATE,MEDDATE,OPDATE,ACPTTIME,EXECTIME,PATFG,ORDCLSTYP,ORDCODE
                ,MEDDEPT,ORDDR,CHADR,EXECDR,WARDNO,QTY,CNT,EXAMTYP,PRNACTYN,DCYN,DCORDSEQ,MKFG,EXAMRTNYN,REGTIME
                                       from BYUN_SOURCE_DATA.dbo.MMEXMORT where PATFG not in ('O','G','H','M') and ORDCLSTYP IN ('C2','C3')) as e1
union all
-- 3. MMREHORT 처치
-- FROMDATE,TODATE, CURECNT 확인 필요  CURECNT (1일횟수) || EXECCNT (전체 치료횟수)

   select
              'MMREHORT' as tbnm
              ,r1.PATNO -- 환자ID
              ,r1.MEDDEPT
              ,r1.PATFG -- 내원구분
              ,null as SEQNO
              ,r1.ORDSEQNO -- 처방순번
              ,r2.EXECSEQ as seq
              ,r1.MEDDATE -- 진료/입원/수술(DSC)/응급실도착일시
              ,r1.ORDDATE -- 처방일자
              ,case
                when r2.ACTINGRETM is not null then cast(r2.ACTINGRETM as date )
                when r2.ACTINGRETM is null and r2.INDTTM is not null then cast(r2.INDTTM as date)
                when (r2.ACTINGRETM is null and r2.INDTTM is null) and r1.FROMDATE is not null then cast(r1.FROMDATE as date)
                ELSE cast(r1.ORDDATE as date)
                end as Procedure_date
              ,case
                when r2.ACTINGRETM is not null then cast(r2.ACTINGRETM as datetime)
                when r2.ACTINGRETM is null and r2.INDTTM is not null then cast(r2.INDTTM as datetime)
                when (r2.ACTINGRETM is null and r2.INDTTM is null) and r1.FROMDATE is not null then cast(r1.FROMDATE as datetime)
                ELSE null
                end as Procedure_datetime
              ,r2.INDTTM
              ,r1.EXAMTYP as TYP -- 처방분류TYP(MM182)
              ,r1.ORDCODE -- 처방코드
              ,null as ORDNAME
              ,case
                when r2.CNT is null or r2.CNT = '0' then  1
                  else r2.CNT
                end as ACTCNT
              ,case
                when r1.CURECNT is null or r1.CURECNT ='0' then 1
                   else r1.CURECNT
                end as CNT  -- 치료횟수(1일)
              ,'1' as R_DAY -- EXECCNT일 경우 CURECNT로 나눠줘야 day 나옴 아니면 datediff(day,From_date, To_date)
              ,null as RETNQTY
              ,r1.ORDDR -- 처방의사
              ,r1.CHADR -- 주치의
              ,r1.PRNACTYN -- PRN실시처방여부
              ,r1.DCYN -- D/C여부
              ,r1.DCORDSEQ -- D/C원처방번호
              ,r1.MKFG -- 처방발생구분(MM003)
              ,r1.REGTIME
              ,r1.WARDNO -- 병동
              ,r2.REJTTIME
              ,case
                  when r1.DCYN = 'Y' then 'N'
                  when r1.DCORDSEQ is not null then 'N'
                  when r1.MKFG not in ('N','P') then 'N'
                  when r2.REJTTIME is not null then 'N'
                  when r2.ACTFG = 'N' then 'N'
                else 'Y'
                end as exc_yn

            from (select
                        PATNO ,ORDDATE,ORDSEQNO,MEDDATE,PATFG,MEDDEPT ,WARDNO ,ORDCLSTYP ,ORDCODE ,ORDDR ,CHADR
                      ,EXECDR ,EXAMTYP ,SITECODE  ,CURECNT ,CUREDAY,FROMDATE ,EXECCNT ,TODATE ,DCYN ,DCORDSEQ ,MKFG,PRNACTYN,REGTIME
                    from BYUN_SOURCE_DATA.dbo.MMREHORT where PATFG in ('I','E','D')) as r1
            left join (select PATNO,ORDDATE,ORDSEQNO,ORDCODE,ORDCLSFG,ADMTIME,EXECSEQ,ACTINGRETM
                       ,INDTTM,CNT,ACTFG,REJTTIME,(ROW_NUMBER() over(order by newid()))+10000 AS Seq from BYUN_SOURCE_DATA.dbo.MNWADACT where ACTFG= 'Y') as r2
                  on r1.PATNO = r2.PATNO and r1.ORDDATE = r2.ORDDATE and r1.ORDSEQNO = r2.ORDSEQNO and r1.ORDCODE = r2.ORDCODE


union all
          select
              'MMREHORT' as tbnm
              ,r1.PATNO -- 환자ID
              ,r1.MEDDEPT -- 진료과
              ,r1.PATFG -- 내원구분
              ,null as SEQNO
              ,r1.ORDSEQNO
              ,null as Seq
              ,r1.MEDDATE -- 진료/입원/수술(DSC)/응급실도착일시
              ,r1.ORDDATE -- 처방일자
              ,case
                when r1.FROMDATE is not null then cast(r1.FROMDATE as date)
                else cast(r1.ORDDATE as date)
                end as Procedure_date
              ,null as Procedure_datetime
              ,null as INDTTM
              ,r1.EXAMTYP as TYP -- 치료분류
              ,r1.ORDCODE -- 처방코드
              ,null as ORDNAME
              ,null as ACTCNT
              ,case
                when r1.CURECNT is null or r1.CURECNT ='0' then 1
                   else r1.CURECNT
                end as CNT
              ,'1' as R_DAY
              ,null as RETNQTY
              ,r1.ORDDR -- 처방의사
              ,r1.CHADR -- 주치의
              ,r1.PRNACTYN
              ,r1.DCYN -- D/C여부
              ,r1.DCORDSEQ -- D/C원처방번호
              ,r1.MKFG -- 처방발생구분(MM003)
              ,r1.REGTIME
              ,r1.WARDNO -- 병동
              ,null as REJTTIME
              ,case
                  when r1.DCYN = 'Y' then 'N'
                  when r1.DCORDSEQ is not null then 'N'
                  when r1.MKFG not in ('N','P') then 'N'
                else 'Y'
                end as exc_yn

            from (select
                        PATNO ,ORDDATE,ORDSEQNO,MEDDATE,PATFG,MEDDEPT ,WARDNO ,ORDCLSTYP ,ORDCODE ,ORDDR ,CHADR
                      ,EXECDR ,EXAMTYP ,SITECODE  ,CURECNT ,CUREDAY,FROMDATE ,EXECCNT ,TODATE ,DCYN ,DCORDSEQ ,MKFG,PRNACTYN,REGTIME
                    from BYUN_SOURCE_DATA.dbo.MMREHORT where PATFG in ('O','G','H','M')) as r1
union all
   select
        'MNBLOODT' as tbnm
        ,o1.PATNO -- 환자번호
        ,o1.MEDDEPT -- 진료과
        ,o1.PATFG
        ,o1.EXECSEQNO as SEQNO -- 실시순번
        ,o1.ORDSEQNO -- 처방순번
        ,o2.EXECSEQ as seq
        ,o2.ADMTIME as MEDDATE
        ,o1.ORDDATE -- 처방일자
        ,case
          when o2.ACTINGRETM is not null then cast(o2.ACTINGRETM as date)
          when o2.ACTINGRETM is null and o2.INDTTM is not null then cast(o2.INDTTM as date)
          when (o2.ACTINGRETM is null and o2.INDTTM is null)  and o1.EXECDATE is not null then cast(o1.EXECDATE as date)
          else cast(o1.ORDDATE as date)
        end as procedure_date
        ,case
          when o2.ACTINGRETM is not null then cast(o2.ACTINGRETM as datetime)
          when o2.ACTINGRETM is null and o2.INDTTM is not null then cast(o2.INDTTM as datetime)
          when (o2.ACTINGRETM is null and o2.INDTTM is null)  and o1.EXECDATE is not null then cast(o1.EXECDATE as datetime)
          else null
        end as procedure_datetime
        ,o2.INDTTM
        ,null as TYP
        ,o1.ORDCODE -- 처방코드
        ,null as ORDNAME
        ,case
          when o2.CNT is null or o2.CNT = '0' then 1
          else o2.CNT
         end as ACTCNT
        ,'1' as CNT
        ,'1' as R_DAY
        ,null as RETNQTY
        ,o1.BLDSID as ORDDR
        ,o1.MEDDR as CHADR
        ,o2.ACTFG as PRNATCYN
        ,o1.RTNYN as DCYN
        ,null as DCORDSEQNO
        ,null as MKFG
        ,o1.REGTIME
        ,null as WARDNO
        ,o1.REJTTIME
        ,case
            when o1.RTNYN = 'Y' then 'N'
            when o1.REJTTIME is not null then 'N'
          else 'Y'
         end as exc_yn
    from (select
                PATNO,ORDDATE,ORDSEQNO,EXECSEQNO,ACTFG,EXECDATE,ORDCODE ,PATFG, BLDSID
                ,MEDDATE,MEDDEPT,MEDDR ,RTNYN ,REJTTIME,REGTIME
          from BYUN_SOURCE_DATA.dbo.MNBLOODT where PATFG in ('I','E','D')) as o1
   left join (select PATNO,ORDDATE,ORDSEQNO,ORDCODE,ORDCLSFG,ADMTIME,EXECSEQ,ACTINGRETM
                       ,INDTTM,CNT,ACTFG,REJTTIME,(ROW_NUMBER() over(order by newid()))+100000 AS Seq from BYUN_SOURCE_DATA.dbo.MNWADACT where ACTFG= 'Y') as o2
     on o1.PATNO = o2.PATNO and o1.ORDDATE = o2.ORDDATE and o1.ORDSEQNO = o2.ORDSEQNO and o1.ORDCODE = o2.ORDCODE

union all

        select
        'MNBLOODT' as tbnm
        ,b1.PATNO -- 환자번호
        ,b1.MEDDEPT
        ,b1.PATFG
        ,b1.EXECSEQNO as SEQNO-- 실시순번
        ,b1.ORDSEQNO
        ,null as Seq
        ,b1.MEDDATE
        ,b1.ORDDATE -- 처방일자
        ,case
          when b1.EXECDATE is not null then cast(b1.EXECDATE as date)
          else cast(b1.ORDDATE as date)
        end as procedure_date
        ,case
          when b1.EXECDATE is not null then cast(b1.EXECDATE as datetime)
          else null
        end as procedure_datetime
        ,null as INDTTM
        ,null as TYP
        ,b1.ORDCODE
        ,null as ORDNAME
        ,null as ACTCNT
        ,'1' as CNT
        ,null as RETNQTY
        ,null as ACTFG
        ,b1.BLDSID as ORDDR
        ,b1.MEDDR as CHADR
        ,null as PRNACTYN
        ,b1.RTNYN as DCYN
        ,null as DCORDSEQNO
        ,null as MKFG
        ,b1.REGTIME
        ,null as WARDNO
        ,b1.REJTTIME
        ,case
            when b1.RTNYN = 'Y' then 'N'
            when b1.REJTTIME is not null then 'N'
          else 'Y'
         end as exc_yn
    from (select
                PATNO,ORDDATE,ORDSEQNO,EXECSEQNO,ACTFG,EXECDATE,ORDCODE ,PATFG
                ,MEDDATE,MEDDEPT,MEDDR ,RTNYN ,REJTTIME,BLDSID, REGTIME
          from BYUN_SOURCE_DATA.dbo.MNBLOODT where PATFG in ('O','G','M','H')) as b1) as p1
 inner join (select PATNO, Btdt from Byun_meta_1M.dbo.Dev_person) as dp1
                on dp1.PATNO = p1.PATNO
 left join (select PATNO, death_dt from Byun_meta_1M.dbo.Dev_death) as dd1
                on dd1.PATNO = p1.PATNO

--> [2019-10-29 22:14:40] 2,800,466 rows affected in 17 s 361 ms
drop table Byun_meta_1M.dbo.Dev_procedure;
select
    *
,case when exc_yn = 'Y' then 'Y'
      when exc_yn = 'N' then 'N'
      else 'N'
  end as include_yn
into Byun_meta_1M.dbo.Dev_procedure
from (select distinct p1.*, v1.uniq_no as visit_id
      from (select distinct
tbnm
,uniq_no
,ordseqno
,other_seqno
,exec_seq
,patno
,meddept
,patfg
,meddate
,orddate
,procedure_date
,procedure_datetime
,ordcode
,cnt
,actcnt
,day
,ordtyp
,ord_gb
,orddr
,wardno
,dc_yn
,prn_yn
,prnact_yn
,exc_yn
,death_err
,Brith_err
,date_err
 from Byun_meta_1M.dbo.Dev_procedure_step1 where patfg in ('I', 'E', 'D') and exc_yn = 'y')as p1
             left join (select uniq_no,
                               patno,
                               visit_date,
                               visit_time,
                               dsch_date,
                               dsch_time,
                               med_dr,
                               med_dept,
                               patfg,
                               cancel_yn
                        from BYUN_META_1M.dbo.Dev_visit
                        where PATFG in ('E', 'I', 'D')
                          and tbnm in ('APIPDSLT')) as v1 on v1.patno = p1.PATNO and v1.visit_time = p1.MEDDATE
                                                               and v1.med_dept = p1.MEDDEPT and v1.patfg = p1.patfg and v1.med_dr = p1.orddr
      union all
      select distinct p1.*, v1.uniq_no as visit_id
      from (select distinct
tbnm
,uniq_no
,ordseqno
,other_seqno
,exec_seq
,patno
,meddept
,patfg
,meddate
,orddate
,procedure_date
,procedure_datetime
,ordcode
,cnt
,actcnt
,day
,ordtyp
,ord_gb
,orddr
,wardno
,dc_yn
,prn_yn
,prnact_yn
,exc_yn
,death_err
,Brith_err
,date_err

            from Byun_meta_1M.dbo.Dev_procedure_step1 where patfg in ('H', 'O') and exc_yn = 'y')as p1
             left join (select uniq_no,
                               patno,
                               visit_date,
                               visit_time,
                               med_dr,
                               med_dept,
                               patfg,
                               cancel_yn
                        from BYUN_META_1M.dbo.Dev_visit
                        where PATFG = 'O'
                          and tbnm in ('AOOPDLST')) as v1
               on v1.patno = p1.PATNO and v1.visit_date = p1.MEDDATE and v1.med_dept = p1.MEDDEPT and
                  v1.med_dr = p1.orddr
  union all
      select distinct p1.*, v1.uniq_no as visit_id
      from (select distinct
 tbnm
,uniq_no
,ordseqno
,other_seqno
,exec_seq
,patno
,meddept
,patfg
,meddate
,orddate
,procedure_date
,procedure_datetime
,ordcode
,cnt
,actcnt
,day
,ordtyp
,ord_gb
,orddr
,wardno
,dc_yn
,prn_yn
,prnact_yn
,exc_yn
,death_err
,Brith_err
,date_err
            from Byun_meta_1M.dbo.Dev_procedure_step1 where patfg in ('G', 'M') and exc_yn = 'y')as p1
             left join (select patno,
                               visit_date,
                               visit_time,
                               med_dr,
                               med_dept,
                               uniq_no,
                               case when patfg = 'S' then 'M' else patfg end as patfg
                        from BYUN_META_1M.dbo.Dev_visit
                        where PATFG in ('G', 'S')  ) as v1
               on v1.patno = p1.PATNO and (cast(v1.visit_date as date) = cast(p1.MEDDATE as date)) and
                  v1.patfg = p1.patfg and v1.med_dr = p1.orddr
     )v;
--> 2,867,590 rows affected in 8 s 451 ms  --> 67,124 row will duplicated 
