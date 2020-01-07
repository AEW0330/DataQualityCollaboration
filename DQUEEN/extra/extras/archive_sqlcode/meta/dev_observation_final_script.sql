--> step 1
select
'observation_emr' as tbnm
,o1.*
, case
		when d1.death_dt is not null and (o1.meddate > d1.death_dt
					or o1.MEDTIME > d1.death_dt or o1.FRMDT > d1.death_dt ) then 'Y'
						else 'N'
					end as Death_error
, case
		when o1.meddate is not null and o1.meddate < p1.Btdt then 'Y'
		when o1.MEDTIME is not null and o1.MEDTIME < p1.Btdt then 'Y'
		when o1.FRMDT is not null and o1.FRMDT < p1.Btdt then 'Y'
		else 'N'
	end as Birth_error
, case
		when o1.meddate > o1.medtime then 'Y'
		when o1.meddate > o1.frmdt then 'Y'
		else 'N'
	end as date_Err


	into Byun_meta_1M.dbo.Dev_observation_step1

from
(select
ROW_NUMBER() over(order by newid()) AS uniq_no
,FRMCLN_KEY as frm_no
,PTNT_NO as patno
,CLN_TYPE as patfg
,MED_DATE as meddate
,MED_TIME as medtime
,FRM_DT as frmdt
,frm_key as frmkey
,FRM_NM as frm_nm
,MRIT_KEY as item_key
,MRIT_NM as item_nm
,ATTR_KEY
,ATTR_NM
,CONT_NM
,INS_TYPE as rslt_typ
,STUS_CD as rslt_cd
,ADD_TXT as rlst_txt
,null as rslt_num
,null as unit
,MED_DEPT as meddept
,MED_DR as meddr
,VLD_GB as include_yn
,source_value
from BYUN_SOURCE_DATA.dbo.Observation_EMR) as o1

inner join (select PATNO, Btdt from BYUN_META_1M.dbo.Dev_person) as p1
		on p1.PATNO = o1.patno
left join (select PATNO,death_dt from BYUN_META_1M.dbo.Dev_death) as d1
		on d1.PATNO = o1.patno

--> [2019-10-30 20:41:52] 2,095,760 rows affected in 8 s 13 ms
--> step2

select * into Byun_meta_1M.dbo.dev_observation from
(select distinct  o1.*,v1.uniq_no as visit_id  from
		(select * from Byun_meta_1M.dbo.Dev_observation_step1
					where patfg in ('H','O') and include_yn = 'Y') as o1
left join (select
                    uniq_no,patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG = 'O' and tbnm in ('AOOPDLST')) as v1
                on v1.patno = o1.PATNO and v1.visit_date = o1.MEDDATE and v1.med_dept = o1.MEDDEPT and v1.med_dr = o1.meddr
union all
select distinct o1.*,v1.uniq_no as visit_id  from
		(select * from Byun_meta_1M.dbo.Dev_observation_step1
					where patfg in ('E','I','D') and include_yn = 'Y') as o1
left join (select
                    uniq_no, patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG in ('E','I','D') and tbnm in ('APIPDSLT')) as v1
                on v1.patno = o1.PATNO and v1.visit_time = o1.MEDDATE
                   and v1.med_dept = o1.MEDDEPT and v1.patfg = o1.patfg
union all
select distinct o1.*,v1.uniq_no as visit_id  from
		(select * from Byun_meta_1M.dbo.Dev_observation_step1
					where patfg in ('E','I','D') and include_yn = 'Y') as o1
left join (select patno, visit_date, visit_time, dsch_date, dsch_time,med_dr, med_dept, uniq_no,cancel_yn, case when patfg ='S' then 'M' else patfg end as patfg
                       from BYUN_META_1M.dbo.Dev_visit
                        where PATFG in ('G','S')) as v1
                on v1.patno = o1.PATNO and (cast(v1.visit_date as date) = cast(o1.MEDDATE as date)) and v1.patfg = o1.patfg)v

-- [2019-10-30 20:48:51] 4,163,001 rows affected in 13 s 314 ms (visit때문에 테이블이 늘어난듯) 