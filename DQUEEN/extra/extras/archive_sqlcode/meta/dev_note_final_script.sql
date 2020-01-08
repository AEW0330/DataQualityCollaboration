select
ROW_NUMBER() over(order by newid()) AS uniq_no
,*
into Byun_meta_1M.dbo.Dev_note_step1
from
(select
					ad1.*
				, case
						when ad1.MED_DATE is not null and ad1.MED_DATE < b1.Btdt then 'Y'
						when ad1.MED_TIME is not null and ad1.MED_TIME < b1.Btdt then 'Y'
						when ad1.FRM_DT is not null and ad1.FRM_DT < b1.Btdt then 'Y'
						else 'N'
					end as Birth_error
				, case
						when d1.death_dt is not null and (ad1.MED_DATE > d1.death_dt
																							 		or ad1.MED_TIME > d1.death_dt
																						 					or ad1.FRM_DT > d1.death_dt ) then 'Y'
						else 'N'
					end as Death_error
				, case
						when br1.VLD_GB is not null and br1.VLD_GB = 'Y' then 'Y'
						when br1.VLD_GB = 'N' or br1.VLD_GB is null then 'N'
						else 'N'
					end as include_yn

			from
						(
						select
									 PTNT_NO
									,FRM_DT
									,MED_DATE
									,MED_TIME
									,MED_DEPT
									,MED_DR
									,DEPT_CD
									,CLN_TYPE
									,FRMCLN_KEY
									,FRM_NM
									,null as GRID_NM
									,MRIT_NM
									,ATTR_NM
									,null as COL1
									,null as COL2
									,null as CONT_NM
									,INS_TYPE
									,NMRC_DATA
									,DT_DATA
									,TXT_DATA
									,null as STUS_CD
									,null as ADD_TXT
							 from BYUN_SOURCE_DATA.dbo.Admission_note_EMR1
									where INS_TYPE in ('DT','NU','RI','TE')

						Union all

						select
									 PTNT_NO
									,FRM_DT
									,MED_DATE
									,MED_TIME
									,MED_DEPT
									,MED_DR
									,DEPT_CD
									,CLN_TYPE
									,FRMCLN_KEY
									,FRM_NM
									,null as GRID_NM
									,MRIT_NM
									,ATTR_NM
									,null as COL1
									,null as COL2
									,CONT_NM
									,INS_TYPE
									,null as NMRC_DATA
									,null as DT_DATA
									,null as TXT_DATA
									,STUS_CD
									,ADD_TXT
						 from BYUN_SOURCE_DATA.dbo.Admission_note_EMR2
								where INS_TYPE = 'CO'

						union all

						select
								 PTNT_NO
								,FRM_DT
								,MED_DATE
								,MED_TIME
								,MED_DEPT
								,MED_DR
								,DEPT_CD
								,CLN_TYPE
								,FRMCLN_KEY
								,FRM_NM
								,GRID_NM
								,MRIT_NM
								,ATTR_NM
								,COL1
								,COL2
								,null as CONT_NM
								,INS_TYPE
								,null as NMRC_DATA
								,DT_DATA
								,TXT_DATA
								,null as STUS_CD
								,null as ADD_TXT
						 from BYUN_SOURCE_DATA.dbo.Admission_note_EMR3
								where INS_TYPE in ('TE','DA') ) as ad1
			inner join (select FRMCLN_KEY, VLD_GB from DW.dbo.DW_MRR_FRM_CLNINFO) as br1 on br1.FRMCLN_KEY = ad1.FRMCLN_KEY
			inner join  (select PATNO, Btdt from Byun_meta_1M.dbo.Dev_person) as  b1 on b1.PATNO = ad1.PTNT_NO
			left join (select PATNO, death_dt from Byun_meta_1M.dbo.Dev_death) as d1 on d1.PATNO = ad1.PTNT_NO

union all

		select
					ad1.*
				, case
						when ad1.MED_DATE is not null and ad1.MED_DATE < b1.Btdt then 'Y'
						when ad1.MED_TIME is not null and ad1.MED_TIME < b1.Btdt then 'Y'
						when ad1.FRM_DT is not null and ad1.FRM_DT < b1.Btdt then 'Y'
						else 'N'
					end as Birth_error
				, case
						when d1.death_dt is not null and (ad1.MED_DATE > d1.death_dt
																							 		or ad1.MED_TIME > d1.death_dt
																						 					or ad1.FRM_DT > d1.death_dt ) then 'Y'
						else 'N'
					end as Death_error
				, case
						when br1.VLD_GB is not null and br1.VLD_GB = 'Y' then 'Y'
						when br1.VLD_GB = 'N' or br1.VLD_GB is null then 'N'
						else 'N'
					end as include_yn

			from
						(
						select
									 PTNT_NO
									,FRM_DT
									,MED_DATE
									,MED_TIME
									,MED_DEPT
									,MED_DR
									,DEPT_CD
									,CLN_TYPE
									,FRMCLN_KEY
									,FRM_NM
									,null as GRID_NM
									,MRIT_NM
									,ATTR_NM
									,null as COL1
									,null as COL2
									,null as CONT_NM
									,INS_TYPE
									,NMRC_DATA
									,DT_DATA
									,TXT_DATA
									,null as STUS_CD
									,null as ADD_TXT
							 from BYUN_SOURCE_DATA.dbo.Progress_note_EMR1
									where INS_TYPE in ('DT','NU','RI','TE')

						Union all

						select
									 PTNT_NO
									,FRM_DT
									,MED_DATE
									,MED_TIME
									,MED_DEPT
									,MED_DR
									,DEPT_CD
									,CLN_TYPE
									,FRMCLN_KEY
									,FRM_NM
									,null as GRID_NM
									,MRIT_NM
									,ATTR_NM
									,null as COL1
									,null as COL2
									,CONT_NM
									,INS_TYPE
									,null as NMRC_DATA
									,null as DT_DATA
									,null as TXT_DATA
									,STUS_CD
									,ADD_TXT
						 from BYUN_SOURCE_DATA.dbo.Progress_note_EMR2
								where INS_TYPE = 'CO'

						union all

						select
								 PTNT_NO
								,FRM_DT
								,MED_DATE
								,MED_TIME
								,MED_DEPT
								,MED_DR
								,DEPT_CD
								,CLN_TYPE
								,FRMCLN_KEY
								,FRM_NM
								,GRID_NM
								,MRIT_NM
								,ATTR_NM
								,COL1
								,COL2
								,null as CONT_NM
								,INS_TYPE
								,null as NMRC_DATA
								,DT_DATA
								,TXT_DATA
								,null as STUS_CD
								,null as ADD_TXT
						 from BYUN_SOURCE_DATA.dbo.Progress_note_EMR3
								where INS_TYPE in ('TE','DA') ) as ad1
			inner join (select FRMCLN_KEY, VLD_GB from DW.dbo.DW_MRR_FRM_CLNINFO) as br1 on br1.FRMCLN_KEY = ad1.FRMCLN_KEY
			inner join  (select PATNO, Btdt from Byun_meta_1M.dbo.Dev_person) as  b1 on b1.PATNO = ad1.PTNT_NO
			left join (select PATNO, death_dt from Byun_meta_1M.dbo.Dev_death) as d1 on d1.PATNO = ad1.PTNT_NO

					union all

			select
						ad1.*
					, case
							when ad1.MED_DATE is not null and ad1.MED_DATE < b1.Btdt then 'Y'
							when ad1.MED_TIME is not null and ad1.MED_TIME < b1.Btdt then 'Y'
							when ad1.FRM_DT is not null and ad1.FRM_DT < b1.Btdt then 'Y'
							else 'N'
						end as Birth_error
					, case
							when d1.death_dt is not null and (ad1.MED_DATE > d1.death_dt
																										or ad1.MED_TIME > d1.death_dt
																												or ad1.FRM_DT > d1.death_dt ) then 'Y'
							else 'N'
						end as Death_error
					, case
							when br1.VLD_GB is not null and br1.VLD_GB = 'Y' then 'Y'
							when br1.VLD_GB = 'N' or br1.VLD_GB is null then 'N'
							else 'N'
						end as include_yn
				from
					(SELECT
							 MRC1.PTNT_NO
							,MRC1.FRM_DT
							,MRC1.MED_DATE
							,MRC1.MED_TIME
							,MRC1.MED_DEPT
							,MRC1.MED_DR
							,MRC1.DEPT_CD
							,MRC1.CLN_TYPE
							,MRC1.FRMCLN_KEY
							,AT1.FRM_NM
							,null as GRID_NM
							,AT1.MRIT_NM
							,AT1.ATTR_NM
							,null as COL1
							,null as COL2
							,null as CONT_NM
							,AT1.INS_TYPE
							,AT1.NMRC_DATA
							,AT1.DT_DATA
							,AT1.TXT_DATA
							,null as STUS_CD
							,null as ADD_TXT

						FROM DW.dbo.DW_MRR_ATTR_CLNINFO AS AT1
							JOIN (SELECT	*	FROM DW.dbo.DW_MRR_FRM_CLNINFO
										WHERE VLD_GB = 'Y') AS MRC1
							ON MRC1.FRMCLN_KEY = AT1.FRMCLN_KEY
						WHERE AT1.FRM_KEY IN
							(SELECT DISTINCT  MF1.FRM_KEY FROM DW.dbo.DW_MRF_FRM AS MF1
							WHERE  MF1.LTYPE = 0001 AND  MF1.MTYPE IN (0007,0008)	AND  MF1.STYPE IN (0025, 0216) )
							AND AT1.INS_TYPE ='TE' AND AT1.MRIT_NM NOT IN('서명','주치의','작성의')
							and AT1.ATTR_NM NOT IN ('입원 주치의', '퇴원 주치의','집도의 1','집도의 2', '보조의 1', '보조의 2','주치의','주치의')) as ad1
					inner join (select FRMCLN_KEY, VLD_GB from DW.dbo.DW_MRR_FRM_CLNINFO) as br1
							on br1.FRMCLN_KEY = ad1.FRMCLN_KEY
					inner join  (select PATNO, Btdt from Byun_meta_1M.dbo.Dev_person) as  b1
							on b1.PATNO = ad1.PTNT_NO
					left join (select PATNO, death_dt from Byun_meta_1M.dbo.Dev_death) as d1
							on d1.PATNO = ad1.PTNT_NO)v
							--> (3,388,787개 행이 영향을 받음) 04:11

select
    *,
  case when meddate > medtime or meddate > frmdt then 'Y'
      when medtime > frmdt then 'Y'
      else 'N'
    end as date_err
into Byun_meta_1M.dbo.dev_clinicalnote

from (
select
    n1.*
   ,v1.uniq_no as visit_id
from
 (select
   FRM_NM as tbnm
  ,uniq_no
  ,FRMCLN_KEY as frm_no
  ,PTNT_NO as patno
  ,MED_DEPT as meddept
  ,MED_DATE as meddate
  ,MED_TIME as medtime
  ,FRM_DT as frmdt
  ,CLN_TYPE as patfg
  ,null as frmkey
  ,FRM_NM
  ,null as grid_key
  ,GRID_NM as grid_nm
  ,null as mrit_key
  ,MRIT_NM asmrit_nm
  ,null as attr_key
  ,ATTR_NM as attr_nm
  ,COL1 as col1
  ,COL2 as col2
  ,CONT_NM as cont_nm
  ,INS_TYPE as rslt_typ
  ,NMRC_DATA as data_num
  ,TXT_DATA as data_txt
  ,ADD_TXT as data_txt2
  ,DT_DATA as data_dt
  ,STUS_CD as rlst_cd
  ,MED_DR as meddr
  ,'N' as cancel_yn
  ,Death_error
  ,Birth_error
  ,include_yn
  ,DEPT_CD
from Byun_meta_1M.dbo.Dev_note_step1
      where include_yn = 'Y' and CLN_TYPE in ('H','O') ) as n1
left join (select
                    uniq_no,patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG = 'O' and tbnm in ('AOOPDLST')) as v1
                on v1.patno = n1.PATNO and v1.visit_date = n1.MEDDATE and v1.med_dept = n1.MEDDEPT and v1.med_dr = n1.meddr
union all
select
    n1.*
   ,v1.uniq_no as visit_id
from
 (select
   FRM_NM as tbnm
  ,uniq_no
  ,FRMCLN_KEY as frm_no
  ,PTNT_NO as patno
  ,MED_DEPT as meddept
  ,MED_DATE as meddate
  ,MED_TIME as medtime
  ,FRM_DT as frmdt
  ,CLN_TYPE as patfg
  ,null as frmkey
  ,FRM_NM
  ,null as grid_key
  ,GRID_NM as grid_nm
  ,null as mrit_key
  ,MRIT_NM asmrit_nm
  ,null as attr_key
  ,ATTR_NM as attr_nm
  ,COL1 as col1
  ,COL2 as col2
  ,CONT_NM as cont_nm
  ,INS_TYPE as rslt_typ
  ,NMRC_DATA as data_num
  ,TXT_DATA as data_txt
  ,ADD_TXT as data_txt2
  ,DT_DATA as data_dt
  ,STUS_CD as rlst_cd
  ,MED_DR as meddr
  ,'N' as cancel_yn
  ,Death_error
  ,Birth_error
  ,include_yn
  ,DEPT_CD
from Byun_meta_1M.dbo.Dev_note_step1
      where include_yn = 'Y' and CLN_TYPE in ('E','I','D') ) as n1
left join (select
                    uniq_no, patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG in ('E','I','D') and tbnm in ('APIPDSLT')) as v1
                on v1.patno = n1.PATNO and v1.visit_time = n1.medtime
                   and v1.med_dept = n1.MEDDEPT and v1.patfg = n1.patfg
union all
select
    n1.*
   ,v1.uniq_no as visit_id
from
 (select
   FRM_NM as tbnm
  ,uniq_no
  ,FRMCLN_KEY as frm_no
  ,PTNT_NO as patno
  ,MED_DEPT as meddept
  ,MED_DATE as meddate
  ,MED_TIME as medtime
  ,FRM_DT as frmdt
  ,CLN_TYPE as patfg
  ,null as frmkey
  ,FRM_NM
  ,null as grid_key
  ,GRID_NM as grid_nm
  ,null as mrit_key
  ,MRIT_NM asmrit_nm
  ,null as attr_key
  ,ATTR_NM as attr_nm
  ,COL1 as col1
  ,COL2 as col2
  ,CONT_NM as cont_nm
  ,INS_TYPE as rslt_typ
  ,NMRC_DATA as data_num
  ,TXT_DATA as data_txt
  ,ADD_TXT as data_txt2
  ,DT_DATA as data_dt
  ,STUS_CD as rlst_cd
  ,MED_DR as meddr
  ,'N' as cancel_yn
  ,Death_error
  ,Birth_error
  ,include_yn
  ,DEPT_CD
from Byun_meta_1M.dbo.Dev_note_step1
      where include_yn = 'Y' and CLN_TYPE in ('G','S') ) as n1
left join (select patno, visit_date, visit_time, dsch_date, dsch_time,med_dr, med_dept, uniq_no,cancel_yn, case when patfg ='S' then 'M' else patfg end as patfg
                       from BYUN_META_1M.dbo.Dev_visit
                        where PATFG in ('G','S')) as v1
                on v1.patno = n1.PATNO and (cast(v1.visit_date as date) = cast(n1.MEDDATE as date)) and v1.patfg = n1.patfg)v

--> (3,503,209개 행이 영향을 받음)