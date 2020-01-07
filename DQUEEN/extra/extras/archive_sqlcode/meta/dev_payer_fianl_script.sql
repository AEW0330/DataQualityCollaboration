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
					when mp1.MEDDATE > d1.death_dt then 'Y'
					when mp1.ORDDATE > d1.death_dt then 'Y'
					when mp1.ADMTIME is not null and mp1.ADMTIME > d1.death_dt then 'Y'
					when mp1.ENDDATE is not null and mp1.ENDDATe > d1.death_dt then 'Y'
				else 'N'
			 end as Death_error

		into BYUN_META_1M.dbo.Dev_payer_plan_step1

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

						from BYUN_SOURCE_DATA.dbo.AOOPCALT_cost
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

						from BYUN_SOURCE_DATA.dbo.APIPCALT_cost
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
				inner join (select PATNO,Btdt from BYUN_META_1M.dbo.Dev_person) as p1
						on p1.PATNO = ar1.PATNO
				left join (select PATNO, death_dt from BYUN_META_1M.dbo.Dev_death) as d1
						on d1.PATNO = ar1.PATNO ;
--> [2019-10-31 17:15:39] 116,334,969 rows affected in 6 m 59 s 601 ms
select * from Byun_Dqueen.dbo.schema_specification where stage_gb = 'meta' and tbnm = 'dev_payer'
select
		'dev_payer_step1' as tbnm
		,ROW_NUMBER() over(order by newid()) AS uniq_no
		,patno
		,meddate
		,orddate
		,enddate as endt
		,datediff(day,meddate,enddate) as day
		,null as isty_start_dt
		,null as Isty_fnsh_dt
		,sugatyp
		,INSNAME as payer_id
		,cancyn as cancel_yn
		,Death_error as death_err
		,Brith_error as birth_err
		,case
			when cast(meddate as date) > orddate then 'Y'
			else 'N'
		end as date_err
		,case when cancyn = 'N' then 'Y'
			else 'N'
			end as include_yn
into BYUN_META_1M.dbo.dev_payer
	from BYUN_META_1M.dbo.Dev_payer_plan_step1;
