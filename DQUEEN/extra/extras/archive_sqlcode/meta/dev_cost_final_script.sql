-- 외래 AOOPCALT
--      select * from DW.dbo.DW_CSCOMCDT
--           where MIDGCODE = 'AC007';
select
     ROW_NUMBER() over(order by newid()) AS uniq_no
    ,cl1.*
    ,case
        when cl1.MEDDATE < p1.btdt then 'Y'
        when cl1.ORDDATE < p1.btdt then 'Y'
        when cl1.ADMTIME < p1.btdt then 'Y'
        when cl1.REQDATE < p1.btdt or cl1.RCPDATE < p1.btdt then 'Y'
      else 'N'
    end as birth_err
    ,case
        when cl1.MEDDATE > d1.death_dt then 'Y'
        when cl1.ORDDATE > d1.death_dt then 'Y'
        when cl1.ADMTIME > d1.death_dt then 'Y'
        when cl1.REQDATE > d1.death_dt or cl1.RCPDATE < d1.death_dt then 'Y'
      else 'N'
    end as Death_err
   ,case
        when cl1.MEDDATE > '2018-01-01' or cl1.MEDDATE < '1994-01-01' then 'Y'
        when cl1.ORDDATE > '2018-01-01' or cl1.ORDDATE < '1994-01-01' then 'Y'
        when cl1.ADMTIME > '2018-01-01' or cl1.ADMTIME < '1994-01-01' then 'Y'
        when cl1.REQDATE > '2018-01-01' or cl1.REQDATE < '1994-01-01'  then 'Y'
        when cl1.RCPDATE > '2018-01-01' or cl1.RCPDATE < '1994-01-01' then 'Y'
      else 'N'
    end as Date_err
into Byun_meta_1M.dbo.Dev_cost_step1
    from
    (select
      'AOOPCALT' as tbnm
      ,cal1.RCPSTAT
      ,cal1.PATNO
      ,cal1.RCPSEQ
      ,cal1.RCPDATE
      ,cal1.SEQNO
      ,cal1.PATFG
      ,cal1.MEDDATE
      ,null as ADMTIME
      ,Cal1.ORDDATE
      ,cal1.DAY
      ,cal1.MEDDEPT
      ,cal1.ORDDR
      ,cal1.MEDDR
      ,cal1.ORDTABLE
      ,cal1.ORDSEQNO
      ,cal1.ORDCODE
      ,cal1.SPCCODE
      ,cal1.SUGACODE
      ,null as EDICODE
      ,cal1.QTY
      ,cal1.ADDQTY
      ,cal1.CNT
      ,cal1.INSTYP	, --급여구분(AI042)
      cal1.SUGAPRICE	, --적용수가단가
      cal1.UNITPRICE	, --단가
      cal1.ONEPRICE
      ,cal1.SPCAMT
      ,cal1.ADDAMT	, --병원가산금액
      cal1.RCPAMT	, --수납금액
      cal1.OWNRAT , -- 본인부담률
      ((cal1.RCPAMT/100)*cal1.OWNRAT) as paid_by_pat
      ,(cal1.RCPAMT-((cal1.RCPAMT/100)*cal1.OWNRAT))as paid_by_payer
      ,cal1.REQDATE
      ,cal1.REQKEY
      ,cal1.SUGATYP
      ,REQYN
      ,null as DRGYN
      ,Case
          when cal1.INSTYP in ('1','7')
          then '비급여'
        Else '청구'
       end as gb_nm
      ,case
        when cal1.RCPSTAT in ('4','7','R')
          then 'N'
        when cal1.REJTTIME is not null
          then 'N'
        else 'Y'
        end as include_yn
    from
       BYUN_SOURCE_DATA.dbo.AOOPCALT_cost as cal1

  union all
-- APIPCALT

      select
          'APIPCALT' as tbnm
          ,'Y' as RCPSTAT
          ,PATNO	--환자ID
          ,RCPSEQ	--수납순번
          ,RCPDATE
          ,SEQNO	--순번
          ,PATFG	--내원구분(AP025)
          ,MEDDATE	--진료일자
          ,ADMTIME	--입원일시
          ,ORDDATE	--처방일자
          ,DAY	--일수
          ,MEDDEPT	--진료과
          ,ORDDR	--처방의사
          ,CHADR as MEDDR --주치의사
          ,ORDTABLE	--처방테이블
          ,ORDSEQNO	--처방내림순번
          ,ORDCODE	--처방코드
          ,SPCCODE	--검체코드
          ,SUGACODE --수가코드
          ,EDICODE	--EDI코드(AIEDICDT)
          ,QTY	--수량
          ,ADDQTY	--가산된수량
          ,CNT	--횟수
          ,INSTYP	--급여구분(AI042)
          ,SUGAPRICE	--적용수가단가
          ,UNITPRICE	--단가
          ,ONEPRICE	--점당단가
          ,SPCAMT
          ,ADDAMT	--병원가산금액
          ,RCPAMT	--수납금액
          ,OWNRAT	--본인부담율
          ,((RCPAMT/100) * OWNRAT) as paid_by_pat
          ,(RCPAMT-((RCPAMT/100)* OWNRAT))as paid_by_payer
          ,REQDATE
          ,null as REQKEY
          ,SUGATYP
          ,REQYN
          ,DRGYN
            ,Case
                when INSTYP in ('1','7')
                then '비급여'
              Else '청구'
             end as gb_nm
            ,case
              when CANCYN = 'Y'
                then 'N'
              when REJTTIME is not null
                then 'N'
              else 'Y'
              end as include_yn
      from BYUN_SOURCE_DATA.dbo.APIPCALT_cost ) as cl1
    inner join (select PATNO, Btdt from Byun_meta_1M.dbo.Dev_person) as p1
          on p1.PATNO = cl1.PATNO
      left join (select PATNO, death_dt from Byun_meta_1M.dbo.Dev_death) as d1
          on d1.PATNO = cl1.PATNO
--> [2019-10-30 22:06:01] 23,737,979 rows affected in 2 m 19 s 711 ms

select * from Byun_Dqueen.dbo.schema_specification where stage_gb = 'meta' and tbnm = 'dev_cost'
use Byun_meta_1M
select * from information_schema.columns where table_name = 'dev_cost_step1';

select
tbnm
,uniq_no
,ORDTABLE as ordtbnm
,RCPSEQ as rcpseq
,SEQNO as seqno
,ORDSEQNO as ordseqno
,PATNO as patno
,PATFG as patfg
,MEDDATE as meddate
,ORDDATE as orddate
,RCPDATE as rcpdate
,DAY as day
,MEDDEPT as meddept
,ORDDR as orddr
,ORDCODE as ordcode
,CNT as cnt
,QTY as qty
,ADDQTY as add_qty
,INSTYP as instyp
,UNITPRICE as unitprocie
,ONEPRICE as onprice
,SPCAMT as spcamt
,ADDAMT as addamt
,RCPAMT as rcpamt
,OWNRAT as ownrat
,paid_by_pat
,paid_by_payer
,SUGATYP as sugatyp
,DRGYN as drg_gb
,RCPSTAT as rcpstat
,null as visit_id
,Death_err
,birth_err
,Date_err
,include_yn
,ADMTIME
,MEDDR
,SPCCODE
,SUGACODE
,EDICODE
,SUGAPRICE
,REQDATE
,REQKEY
,REQYN
,gb_nm
into Byun_meta_1M.dbo.dev_cost
from Byun_meta_1M.dbo.Dev_cost_step1;
--> 테이블과의 매핑은 추후 확인
--> [2019-10-30 22:37:13] 23,737,979 rows affected in 55 s 379 ms

