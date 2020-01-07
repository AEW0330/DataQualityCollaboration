
/*
select
PATNO -- ,환자ID
,PATFG
,RCPDATE -- ,수납일자
,MEDDATE -- ,진료일자
,ORDDATE -- ,처방일자
,DAY -- ,일수
,INSTYP -- ,급여구분(AI042)
,REJTTIME -- ,취소일시
,REQDATE -- ,청구일자
,SUGATYP -- ,수가적용구분(AI050)
,OIFG -- ,외래/입원구분
into Byun_origin_Rand_2M.dbo.Payer_AOOPCALT
from DW.dbo.DW_AOOPCALT
	 where PATNO in (select distinct PATNO from Byun_meta_2M.dbo.Dev_person);

--> APIPCALT
select
PATNO -- ,환자ID
,PATFG
,MEDDATE
,ADMTIME -- ,입원일시
,RCPSEQ -- ,수납순번
,RCPTYP -- ,수납구분(AC007)
,ORDDATE -- ,처방일자
,MEDDEPT -- ,진료과
,DAY -- ,일수
,REJTTIME -- , 취소일자
,SUGATYP -- ,수가적용구분
,SUGACODE -- ,수가코드
,CANCYN -- ,취소여부
,STARTTIME -- ,시작일시
,ENDTIME -- ,종료일시
, INSTYP
, REQDATE
, RCPDATE
into Byun_origin_Rand_2M.dbo.Payer_APIPCALT
from DW.dbo.DW_APIPCALT
where PATNO in (select distinct PATNO from Byun_meta_2M.dbo.Dev_person);
*/


---
		select
			 mp1.*
			,ar1.INSNAME
			,case
					when mp1.MEDDATE < p1.Btdt then 'Y'
					when mp1.ORDDATE < p1.Btdt then 'Y'
					when mp1.ADMTIME is not null and mp1.ADMTIME < p1.Btdt then 'Y'
					when mp1.ENDDATE is not null and mp1.ENDDATe < p1.Btdt then 'Y'
				else 'N'
			 end as Brith_error
			,case
					when mp1.MEDDATE > d1.DIEDATE then 'Y'
					when mp1.ORDDATE > d1.DIEDATE then 'Y'
					when mp1.ADMTIME is not null and mp1.ADMTIME > d1.DIEDATE then 'Y'
					when mp1.ENDDATE is not null and mp1.ENDDATe > d1.DIEDATE then 'Y'
				else 'N'
			 end as Death_error

		into Byun_meta_2M.dbo.Dev_payer_plan

			from
				(select
					 distinct
						 PATNO
						,MEDDATE
						,ORDDATE
						,ADMTIME
						,DAY
						,dateadd(day,day,ORDDATE) as ENDDATE
						,SUGATYP
						,CANCYN
				from
						(select

								 PATNO -- ,환자ID
								,PATFG
								,OIFG -- ,외래/입원구분
								,MEDDATE -- ,진료일자
								,ORDDATE -- ,처방일자
								,null as ADMTIME
								,DAY -- ,일수
								,SUGATYP -- ,수가적용구분(AI050)
								,INSTYP -- ,급여구분(AI042)
								,REQDATE -- ,청구일자
								,RCPDATE -- ,수납일자
								,null as STARTTIME
								,null as ENDTIME
								,REJTTIME -- ,취소일시
								,case
									when REJTTIME is not null then 'Y'
									else 'N'
								 end as CANCYN

						from Byun_origin_Rand_2M.dbo.Payer_AOOPCALT
							where SUGATYP = '1' and INSTYP not in ('*')
					union all

						select

								 PATNO -- ,환자ID
								,PATFG
								,null as OIFG
								,MEDDATE
								,ORDDATE -- ,처방일자
								,ADMTIME -- ,입원일시
								,DAY -- ,일수
								,SUGATYP -- ,수가적용구분
								,INSTYP
								,REQDATE
								,RCPDATE
								,STARTTIME -- ,시작일시
								,ENDTIME -- ,종료일시
								,REJTTIME -- , 취소일자
								,case
									when CANCYN is not null then CANCYN
									when CANCYN is null and REJTTIME is not null then 'Y'
									when REJTTIME is not null then 'Y'
									else 'N'
								 end as CANCYN  -- 취소여부

						from Byun_origin_Rand_2M.dbo.Payer_APIPCALT
								where SUGATYP = '1' and INSTYP not in ('*'))v

union all

				select
					 distinct
						 PATNO
						,MEDDATE
						,ORDDATE
						,ADMTIME
						,DAY
						,dateadd(day,day,ORDDATE) as ENDDATE
						,SUGATYP
						,CANCYN
				from
						(select

								 PATNO -- ,환자ID
								,PATFG
								,OIFG -- ,외래/입원구분
								,MEDDATE -- ,진료일자
								,ORDDATE -- ,처방일자
								,null as ADMTIME
								,DAY -- ,일수
								,SUGATYP -- ,수가적용구분(AI050)
								,INSTYP -- ,급여구분(AI042)
								,REQDATE -- ,청구일자
								,RCPDATE -- ,수납일자
								,null as STARTTIME
								,null as ENDTIME
								,REJTTIME -- ,취소일시
								,case
									when REJTTIME is not null then 'Y'
									else 'N'
								 end as CANCYN

						from Byun_origin_Rand_2M.dbo.Payer_AOOPCALT
							where SUGATYP not in ('1') and INSTYP not in ('*')
					union all

						select

								 PATNO -- ,환자ID
								,PATFG
								,null as OIFG
								,MEDDATE
								,ORDDATE -- ,처방일자
								,ADMTIME -- ,입원일시
								,DAY -- ,일수
								,SUGATYP -- ,수가적용구분
								,INSTYP
								,REQDATE
								,RCPDATE
								,STARTTIME -- ,시작일시
								,ENDTIME -- ,종료일시
								,REJTTIME -- , 취소일자
								,case
									when CANCYN is not null then CANCYN
									when CANCYN is null and REJTTIME is not null then 'Y'
									when REJTTIME is not null then 'Y'
									else 'N'
								 end as CANCYN  -- 취소여부

						from Byun_origin_Rand_2M.dbo.Payer_APIPCALT
								where SUGATYP not in ('1') and INSTYP not in ('*'))v) as mp1
				left join (select PATNO,INSNAME from DW.dbo.DW_AIREQBAT ) as ar1
						on mp1.PATNO = ar1.PATNO
				inner join (select PATNO,Btdt from Byun_meta_2M.dbo.Dev_person) as p1
						on p1.PATNO = ar1.PATNO
				left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as d1
						on d1.PATNO = ar1.PATNO ;
