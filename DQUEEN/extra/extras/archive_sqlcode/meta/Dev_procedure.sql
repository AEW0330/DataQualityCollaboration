
select
        ROW_NUMBER() over(order by newid()) AS uniq_num
       , p1. *
        ,case
            when cast(p1.MEDDATE as datetime2) <= cast(dp1.Btdt as datetime2) then 'Y'
            when cast(p1.ORDDATE as datetime2) is not null and cast(p1.ORDDATE as datetime2) <= cast(dp1.Btdt as datetime2) then 'Y'
            when p1.procedure_date is not null and cast(p1.procedure_date as datetime2) <= cast(dp1.Btdt as datetime2) then 'Y'
            when p1.procedure_datetime is not null and cast(p1.procedure_datetime as datetime2) <= cast(dp1.Btdt as datetime2) then 'Y'
            when p1.REGTIME is not null and cast(p1.REGTIME as datetime2) < cast(dp1.Btdt as datetime2) then 'Y'
            else 'N'
         end as Brith_err
        ,case
            when cast(p1.MEDDATE as datetime2) > cast(dd1.DIEDATE as datetime2) then 'Y'
            when p1.ORDDATE is not null and cast(p1.ORDDATE as datetime2) > cast(dd1.DIEDATE as datetime2) then 'Y'
            when p1.procedure_date is not null and cast(p1.procedure_date as datetime2) > cast(dd1.DIEDATE as datetime2) then 'Y'
            when p1.procedure_datetime is not null and cast(p1.procedure_datetime as datetime2) > cast(dd1.DIEDATE as datetime2) then 'Y'
             when p1.REGTIME is not null and cast(p1.REGTIME as datetime2) > cast(dd1.DIEDATE as datetime2) then 'Y'
            else 'N'
         end as death_err
        ,case
            when cast(p1.MEDDATE as datetime2) > cast(p1.ORDDATE as datetime2) then 'Y'
            when cast(p1.ORDDATE as datetime2) > cast(p1.procedure_date as datetime2) or cast(p1.ORDDATE as datetime2) > cast(p1.procedure_datetime as datetime)  then 'Y'
            when cast(p1.MEDDATE as datetime2) > cast(p1.REGTIME as datetime2) then 'Y'
          else 'N'
        end as date_err
into Byun_meta_2M.dbo.Dev_procedure2
from
(
--입원 응급 통원수술
  select
       m1.*
  from
    (select
          m1.PATNO
        , m1.MEDDEPT
        , m1.PATFG
        , m1.SEQNO
        , m1.ORDSEQNO -- 처방순번
        , m2.Seq
        , m1.MEDDATE
        , m1.ORDDATE
        , case
            when m2.INDTTM is not null then cast(m2.INDTTM as date)
            when m1.ANETHSTM is not null then cast(m1.ANETHSTM as date)
            when m1.ANETHSTM is null and m1.OPDATE is not null then cast(m1.OPDATE as date)
            when m1.ANETHSTM is null and m1.OPDATE is null then cast(m1.ORDDATE as date)
          ELSE cast(m1.MEDDATE as date)
          end as Procedure_date
        , case
            when m2.INDTTM is not null then m2.INDTTM
            when m1.ANETHSTM is not null then m1.ANETHSTM
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
            when m1.DCYN = 'Y' then 'Y'
            when m1.DCORDSEQ is not null then 'Y'
            when m1.MKFG not in ('N','P') then 'Y'
            when m2.REJTTIME is not null then 'Y'
           -- when m1.PRNACTYN = 'Y' then 'Y'
          else 'N'
          end as exc_yn
   from
     (select *  from Byun_origin_Rand_2M.dbo.MMTRTORT
      WHERE ORDCLSTYP IN ('D1','D3','D4') and PATFG in ('I','E','D')) as m1
      left join (select PATNO,ORDDATE,ORDSEQNO,ORDCODE,ORDCLSFG,ADMTIME
                       ,INDTTM,UPDTTM,ENDDTTM,ACTINGRETM,CNT,ACTFG,REJTTIME, (ROW_NUMBER() over(order by newid()))+100 AS Seq
                 from Byun_origin_Rand_2M.dbo.MNWADACT where ACTFG= 'Y') as m2
    on m1.PATNO = m2.PATNO
    where m1.ORDDATE = m2.ORDDATE and m1.ORDSEQNO = m2.ORDSEQNO and m1.ORDCODE = m2.ORDCODE

    union all
-- 외래
       select
          m1.PATNO
        , m1.MEDDEPT
        , m1.PATFG
        , m1.SEQNO
        , m1.ORDSEQNO -- 처방순번
        ,null as Seq
        , m1.MEDDATE
        , m1.ORDDATE
        , case
            when m1.ANETHSTM is not null then cast(m1.ANETHSTM as date)
            when m1.ANETHSTM is null and m1.OPDATE is not null then cast(m1.OPDATE as date)
            when m1.ANETHSTM is null and m1.OPDATE is null then cast(m1.ORDDATE as date)
          ELSE cast(m1.MEDDATE as date)
          end as Procedure_date
        , case
            when m1.ANETHSTM is not null then m1.ANETHSTM
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
            when m1.DCYN = 'Y' then 'Y'
            when m1.DCORDSEQ is not null then 'Y'
            when m1.MKFG not in ('N','P') then 'Y'
            --when m2.REJTTIME is not null then 'Y'
            when m1.PRNACTYN = 'Y' then 'Y'
          else 'N'
          end as exc_yn
   from
     (select *  from Byun_origin_Rand_2M.dbo.MMTRTORT
      WHERE ORDCLSTYP IN ('D1','D3','D4') and PATFG not in ('I','E','D')) as m1 )as m1
union all
-- 2.MMEXMORT 검사(검체검사제외)

  select
         e1.PATNO  --환자ID
        ,e1.MEDDEPT  --,진료과
        ,e1.PATFG  --내원구분
        ,null as SEQNO
        ,e1.ORDSEQNO --처방순번
        ,e2.Seq
        ,e1.MEDDATE  -- 진료/입원/수술(DSC)/응급실도착일시
        ,e1.ORDDATE  -- 처방일자
        ,case
          when e2.INDTTM is not null then cast(e2.INDTTM as date)
          when e1.EXECTIME is not null then cast(e1.EXECTIME as date)
        else e1.ORDDATE
        end as  procedure_date
        ,case
          when e2.INDTTM is not null then cast(e2.INDTTM as datetime2)
          when e1.EXECTIME is not null then cast(e1.EXECTIME as datetime2)
        else e1.ORDDATE
        end as  procedure_datetime
        ,e2.INDTTM
        ,e1.EXAMTYP as typ  --,검사분류
        ,e1.ORDCODE  --,처방코드
        ,null as ORDNAME
        ,case
           when e2.CNT is null or e2.CNT = '0' then  1
           else e2.CNT
          end as ACTCNT
        ,case
            when e1.CNT is null or e1.CNT = '0' then  1
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
            when e1.DCYN = 'Y' then 'Y'
            when e1.DCORDSEQ is not null then 'Y'
            when e1.MKFG not in ('N','P') then 'Y'
            when e2.REJTTIME is not null then 'Y'
            when e1.EXAMRTNYN = 'Y' then 'Y'--,검사반환여부
            -- when m1.PRNACTYN = 'Y' then 'Y'
          else 'N'
          end as exc_yn
    from (select PATNO,ORDSEQNO,ORDDATE,MEDDATE,OPDATE,ACPTTIME,EXECTIME,PATFG,ORDCLSTYP,ORDCODE
                ,MEDDEPT,ORDDR,CHADR,EXECDR,WARDNO,QTY,CNT,EXAMTYP,PRNACTYN,DCYN,DCORDSEQ,MKFG,EXAMRTNYN,REGTIME
                                       from Byun_origin_Rand_2M.dbo.MMEXMORT where PATFG in ('I','E','D') and ORDCLSTYP IN ('C2','C3')) as e1
    left join (select PATNO,ORDDATE,ORDSEQNO,ORDCODE,ORDCLSFG,ADMTIME
                       ,INDTTM,CNT,ACTFG,REJTTIME,(ROW_NUMBER() over(order by newid()))+1000 AS Seq from Byun_origin_Rand_2M.dbo.MNWADACT where ACTFG= 'Y') as e2
      on e1.PATNO = e2.PATNO and e1.ORDDATE = e2.ORDDATE and e1.ORDSEQNO = e2.ORDSEQNO and e1.ORDCODE = e2.ORDCODE


union all

       select
         e1.PATNO  --,환자ID
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
            when e1.CNT is null or e1.CNT = '0' then  1
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
            when e1.DCYN = 'Y' then 'Y'
            when e1.DCORDSEQ is not null then 'Y'
            when e1.MKFG not in ('N','P') then 'Y'
            when e1.EXAMRTNYN = 'Y' then 'Y'
           -- when e2.REJTTIME is not null then 'Y'
            when e1.PRNACTYN = 'Y' then 'Y'
          else 'N'
          end as exc_yn
    from (select PATNO,ORDSEQNO,ORDDATE,MEDDATE,OPDATE,ACPTTIME,EXECTIME,PATFG,ORDCLSTYP,ORDCODE
                ,MEDDEPT,ORDDR,CHADR,EXECDR,WARDNO,QTY,CNT,EXAMTYP,PRNACTYN,DCYN,DCORDSEQ,MKFG,EXAMRTNYN,REGTIME
                                       from Byun_origin_Rand_2M.dbo.MMEXMORT where PATFG not in ('I','E','D') and ORDCLSTYP IN ('C2','C3')) as e1
union all
-- 3. MMREHORT 처치
-- FROMDATE,TODATE, CURECNT 확인 필요  CURECNT (1일횟수) || EXECCNT (전체 치료횟수)

   select
               r1.PATNO -- 환자ID
              ,r1.MEDDEPT
              ,r1.PATFG -- 내원구분
              ,null as SEQNO
              ,r1.ORDSEQNO -- 처방순번
              ,r2.Seq
              ,r1.MEDDATE -- 진료/입원/수술(DSC)/응급실도착일시
              ,r1.ORDDATE -- 처방일자
              ,case
                when r2.INDTTM is not null then cast(r2.INDTTM as date)
                when r1.FROMDATE is not null then cast(r1.FROMDATE as date)
                ELSE cast(r1.ORDDATE as date)
                end as Procedure_date
              ,case
                when r2.INDTTM is not null then cast(r2.INDTTM as datetime2)
                when r1.FROMDATE is not null then cast(r1.FROMDATE as datetime2)
                ELSE null
                end as Procedure_datetime
              ,r2.INDTTM
              ,r1.EXAMTYP as TYP -- 처방분류TYP(MM182)
              ,r1.ORDCODE -- 처방코드
              ,null as ORDNAME
              ,case
                when r2.CNT is null or r2.CNT = '0' then  1
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
                  when r1.DCYN = 'Y' then 'Y'
                  when r1.DCORDSEQ is not null then 'Y'
                  when r1.MKFG not in ('N','P') then 'Y'
                  when r2.REJTTIME is not null then 'Y'
                  when r2.ACTFG = 'N' then 'Y'
                else 'N'
                end as exc_yn

            from (select
                        PATNO ,ORDDATE,ORDSEQNO,MEDDATE,PATFG,MEDDEPT ,WARDNO ,ORDCLSTYP ,ORDCODE ,ORDDR ,CHADR
                      ,EXECDR ,EXAMTYP ,SITECODE  ,CURECNT ,CUREDAY,FROMDATE ,EXECCNT ,TODATE ,DCYN ,DCORDSEQ ,MKFG,PRNACTYN,REGTIME
                    from Byun_origin_Rand_2M.dbo.MMREHORT where PATFG in ('I','E','D')) as r1
            left join (select PATNO,ORDDATE,ORDSEQNO,ORDCODE,ORDCLSFG,ADMTIME
                       ,INDTTM,CNT,ACTFG,REJTTIME,(ROW_NUMBER() over(order by newid()))+10000 AS Seq from Byun_origin_Rand_2M.dbo.MNWADACT where ACTFG= 'Y') as r2
                  on r1.PATNO = r2.PATNO and r1.ORDDATE = r2.ORDDATE and r1.ORDSEQNO = r2.ORDSEQNO and r1.ORDCODE = r2.ORDCODE


union all

              select

               r1.PATNO -- 환자ID
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
              ,case
                when r1.FROMDATE is not null then cast(r1.FROMDATE as datetime2)
                else null
                end as Procedure_datetime
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
                  when r1.DCYN = 'Y' then 'Y'
                  when r1.DCORDSEQ is not null then 'Y'
                  when r1.MKFG not in ('N','P') then 'Y'
                 -- when r2.REJTTIME is not null then 'Y'
                 -- when r2.ACTFG = 'N' then 'Y'
                else 'N'
                end as exc_yn

            from (select
                        PATNO ,ORDDATE,ORDSEQNO,MEDDATE,PATFG,MEDDEPT ,WARDNO ,ORDCLSTYP ,ORDCODE ,ORDDR ,CHADR
                      ,EXECDR ,EXAMTYP ,SITECODE  ,CURECNT ,CUREDAY,FROMDATE ,EXECCNT ,TODATE ,DCYN ,DCORDSEQ ,MKFG,PRNACTYN,REGTIME
                    from Byun_origin_Rand_2M.dbo.MMREHORT where PATFG not in ('I','E','D')) as r1
union all
   select
         o1.PATNO -- 환자번호
        ,o1.MEDDEPT -- 진료과
        ,o1.PATFG
        ,o1.EXECSEQNO as SEQNO -- 실시순번
        ,o1.ORDSEQNO -- 처방순번
        ,o2.Seq
        ,o2.ADMTIME as MEDDATE
        ,o1.ORDDATE -- 처방일자
        ,case
          when o2.INDTTM is not null then cast(o2.INDTTM as date)
          when o1.EXECDATE is not null then cast(o1.EXECDATE as date)
          else cast(o1.ORDDATE as date)
        end as procedure_date
        ,case
          when o2.INDTTM is not null then cast(o2.INDTTM as datetime2)
          when o1.EXECDATE is not null then cast(o1.EXECDATE as datetime2)
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
            when o1.RTNYN = 'Y' then 'Y'
            when o1.REJTTIME is not null then 'Y'
          else 'N'
         end as exc_yn
    from (select
                PATNO,ORDDATE,ORDSEQNO,EXECSEQNO,ACTFG,EXECDATE,ORDCODE ,PATFG, BLDSID
                ,MEDDATE,MEDDEPT,MEDDR ,RTNYN ,REJTTIME,REGTIME
          from Byun_origin_Rand_2M.dbo.MNBLOODT where PATFG in ('I','E','D')) as o1
   left join (select PATNO,ORDDATE,ORDSEQNO,ORDCODE,ORDCLSFG,ADMTIME
                       ,INDTTM,CNT,ACTFG,REJTTIME,(ROW_NUMBER() over(order by newid()))+100000 AS Seq from Byun_origin_Rand_2M.dbo.MNWADACT where ACTFG= 'Y') as o2
     on o1.PATNO = o2.PATNO and o1.ORDDATE = o2.ORDDATE and o1.ORDSEQNO = o2.ORDSEQNO and o1.ORDCODE = o2.ORDCODE

union all

        select
         b1.PATNO -- 환자번호
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
          when b1.EXECDATE is not null then cast(b1.EXECDATE as datetime2)
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
            when b1.RTNYN = 'Y' then 'Y'
            when b1.REJTTIME is not null then 'Y'
          else 'N'
         end as exc_yn
    from (select
                PATNO,ORDDATE,ORDSEQNO,EXECSEQNO,ACTFG,EXECDATE,ORDCODE ,PATFG
                ,MEDDATE,MEDDEPT,MEDDR ,RTNYN ,REJTTIME,BLDSID, REGTIME
          from Byun_origin_Rand_2M.dbo.MNBLOODT where PATFG not in ('I','E','D')) as b1) as p1
 inner join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as dp1
                on dp1.PATNO = p1.PATNO
 left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as dd1
                on dd1.PATNO = p1.PATNO