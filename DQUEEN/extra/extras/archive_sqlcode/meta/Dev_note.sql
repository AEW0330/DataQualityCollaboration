

--
select
* into Byun_meta_2M.dbo.Dev_Note
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
						when d1.DIEDATE is not null and (ad1.MED_DATE > d1.DIEDATE
																							 		or ad1.MED_TIME > d1.DIEDATE
																						 					or ad1.FRM_DT > d1.DIEDATE ) then 'Y'
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
							 from Byun_origin_Rand_2M.dbo.Admission_note_EMR1
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
						 from Byun_origin_Rand_2M.dbo.Admission_note_EMR2
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
						 from Byun_origin_Rand_2M.dbo.Admission_note_EMR3
								where INS_TYPE in ('TE','DA') ) as ad1
			inner join (select FRMCLN_KEY, VLD_GB from DW.dbo.DW_MRR_FRM_CLNINFO) as br1 on br1.FRMCLN_KEY = ad1.FRMCLN_KEY
			inner join  (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as  b1 on b1.PATNO = ad1.PTNT_NO
			left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as d1 on d1.PATNO = ad1.PTNT_NO

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
						when d1.DIEDATE is not null and (ad1.MED_DATE > d1.DIEDATE
																							 		or ad1.MED_TIME > d1.DIEDATE
																						 					or ad1.FRM_DT > d1.DIEDATE ) then 'Y'
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
							 from Byun_origin_Rand_2M.dbo.Progress_note_EMR1
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
						 from Byun_origin_Rand_2M.dbo.Progress_note_EMR2
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
						 from Byun_origin_Rand_2M.dbo.Progress_note_EMR3
								where INS_TYPE in ('TE','DA') ) as ad1
			inner join (select FRMCLN_KEY, VLD_GB from DW.dbo.DW_MRR_FRM_CLNINFO) as br1 on br1.FRMCLN_KEY = ad1.FRMCLN_KEY
			inner join  (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as  b1 on b1.PATNO = ad1.PTNT_NO
			left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as d1 on d1.PATNO = ad1.PTNT_NO

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
							when d1.DIEDATE is not null and (ad1.MED_DATE > d1.DIEDATE
																										or ad1.MED_TIME > d1.DIEDATE
																												or ad1.FRM_DT > d1.DIEDATE ) then 'Y'
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
					inner join  (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as  b1
							on b1.PATNO = ad1.PTNT_NO
					left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as d1
							on d1.PATNO = ad1.PTNT_NO)v

--> [2019-05-30 13:37:33] 68424793 rows affected in 8 m 45 s 163 ms