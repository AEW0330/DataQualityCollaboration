
select

	 o1.*

				, case
						when o1.MED_DATE is not null and o1.MED_DATE < p1.Btdt then 'Y'
						when o1.MED_TIME is not null and o1.MED_TIME < p1.Btdt then 'Y'
						when o1.FRM_DT is not null and o1.FRM_DT < p1.Btdt then 'Y'
						else 'N'
					end as Birth_error
				, case
						when d1.DIEDATE is not null and (o1.MED_DATE > d1.DIEDATE
																							 		or o1.MED_TIME > d1.DIEDATE
																						 					or o1.FRM_DT > d1.DIEDATE ) then 'Y'
						else 'N'
					end as Death_error
				, 'Y' as include_yn

		into Byun_meta_2M.dbo.Dev_Observation


from Byun_origin_Rand_2M.dbo.Observation_EMR as o1

inner join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as p1
		on p1.PATNO = o1.PTNT_NO
left join (select PATNO,DIEDATE from Byun_meta_2M.dbo.Dev_death) as d1
		on d1.PATNO = o1.PTNT_NO;

--> [2019-05-30 13:52:32] 42,314,526 rows affected in 4 m 40 s 777 ms

