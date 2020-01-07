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

SELECT *
 INTO BYUN_SOURCE_DATA.dbo.pre_meta_measurement_II
FROM
(
--1. ü��
SELECT
	'MNVITALT' AS TABLENM
	,PATNO
	,ADMTIME
	,RECTIME
	,'BODYTEMP' AS ORDCODE
	,'BODYTEMP' AS SPCCODE
	,REGID
	,EDITID
	,'[degC]' AS UNIT	--�˻�������
	,BODYTEMP AS VALUE_AS_NUMBER
FROM Byun_origin_Rand_2M.dbo.MNVITALT
WHERE BODYTEMP > 0

UNION ALL

--2. �ƹ�
SELECT
	'MNVITALT' AS TABLENM
	,PATNO
	,ADMTIME
	,RECTIME
	,'PULSE' AS ORDCODE
	,'PULSE' AS SPCCODE
	,REGID
	,EDITID
	,'/min' AS UNIT	--�˻�������
	,PULSE AS VALUE_AS_NUMBER
FROM BYUN_SOURCE_DATA.dbo.MNVITALT
WHERE PULSE > 0

UNION ALL

--3. ȣ��
SELECT
	'MNVITALT' AS TABLENM
	,PATNO
	,ADMTIME
	,RECTIME
	,'BREATH' AS ORDCODE
	,'BREATH' AS SPCCODE
	,REGID
	,EDITID
	,'/min' AS UNIT	--�˻�������
	,BREATH AS VALUE_AS_NUMBER
FROM BYUN_SOURCE_DATA.dbo.MNVITALT
WHERE BREATH > 0

UNION ALL

--4. ü��
SELECT
	'MNVITALT' AS TABLENM
	,PATNO
	,ADMTIME
	,RECTIME
	,'WEIGHT' AS ORDCODE
	,'WEIGHT' AS SPCCODE
	,REGID
	,EDITID
	,'kg' AS UNIT	--�˻�������
	,WEIGHT AS VALUE_AS_NUMBER
FROM BYUN_SOURCE_DATA.dbo.MNVITALT
WHERE WEIGHT > 0

UNION ALL

--5. ����
SELECT
	'MNVITALT' AS TABLENM
	,PATNO
	,ADMTIME
	,RECTIME
	,'HEIGHT' AS ORDCODE
	,'HEIGHT' AS SPCCODE
	,REGID
	,EDITID
	,'cm' AS UNIT	--�˻�������
	,HEIGHT AS VALUE_AS_NUMBER
FROM BYUN_SOURCE_DATA.dbo.MNVITALT
WHERE HEIGHT > 0

UNION ALL

--6. SBP (BPH)
SELECT
	'MNVITALT' AS TABLENM
	,PATNO
	,ADMTIME
	,RECTIME
	,'SYSTOLICBP' AS ORDCODE
	,'SYSTOLICBP' AS SPCCODE
	,REGID
	,EDITID
	,'mm[Hg]' AS UNIT	--�˻�������
	,BPH AS VALUE_AS_NUMBER
FROM BYUN_SOURCE_DATA.dbo.MNVITALT
WHERE BPH > 0

UNION ALL

--7. DBP (BPL)
SELECT
	'MNVITALT' AS TABLENM
	,PATNO
	,ADMTIME
	,RECTIME
	,'DIASTOLICBP' AS ORDCODE
	,'DIASTOLICBP' AS SPCCODE
	,REGID
	,EDITID
	,'mm[Hg]' AS UNIT	--�˻�������
	,BPL AS VALUE_AS_NUMBER
FROM BYUN_SOURCE_DATA.dbo.MNVITALT
WHERE BPL > 0
) S
--

SELECT *
 INTO BYUN_SOURCE_DATA.dbo.pre_meta_measurement_IV
FROM
(
--1. HEIGHT, HEIGHT
SELECT
	'MRR_ATTR_CLNINFO' AS TABLENM
	,A.FRMCLN_KEY
	,B.PTNT_NO
	,B.MED_DATE
	,B.MED_TIME
	,B.FRM_DT
	,B.MED_DEPT
	,B.MED_DR
	,B.CLN_TYPE -- PATFG ��� (I, O, ..)
	,'HEIGHT' AS ORDCODE
	,'HEIGHT' AS SPCCODE
	,'cm' AS UNIT
	,A.FRM_KEY
	,A.MRIT_KEY
	,A.ATTR_KEY
	,A.INS_TYPE
	,A.NMRC_DATA
	,A.DT_DATA
	,A.TXT_DATA
	,CASE WHEN ISNUMERIC(A.NMRC_DATA)=1 THEN REPLACE(REPLACE(REPLACE(A.NMRC_DATA,' ',''),',',''),'+','')
				ELSE NULL
	 END AS VALUE_AS_NUMBER
	,B.VLD_GB
FROM DW.dbo.DW_MRR_ATTR_CLNINFO AS A
	JOIN DW.dbo.DW_MRR_FRM_CLNINFO AS B
	ON A.FRMCLN_KEY = B.FRMCLN_KEY
WHERE A.ATTR_KEY IN ('5128507') and B.PTNT_NO in (select distinct patno from BYUN_SOURCE_DATA.dbo.ACPATBAT)
			AND A.NMRC_DATA IS NOT NULL

UNION ALL

--2. WEIGHT, WEIGHT
SELECT
	'MRR_ATTR_CLNINFO' AS TABLENM
	,A.FRMCLN_KEY
	,B.PTNT_NO
	,B.MED_DATE
	,B.MED_TIME
	,B.FRM_DT
	,B.MED_DEPT
	,B.MED_DR
	,B.CLN_TYPE -- PATFG ��� (I, O, ..)
	,'WEIGHT' AS ORDCODE
	,'WEIGHT' AS SPCCODE
	,'kg' AS UNIT
	,A.FRM_KEY
	,A.MRIT_KEY
	,A.ATTR_KEY
	,A.INS_TYPE
	,A.NMRC_DATA
	,A.DT_DATA
	,A.TXT_DATA
	,CASE WHEN ISNUMERIC(A.NMRC_DATA)=1 THEN REPLACE(REPLACE(REPLACE(A.NMRC_DATA,' ',''),',',''),'+','')
				ELSE NULL
	 END AS VALUE_AS_NUMBER
	,B.VLD_GB
FROM DW.dbo.DW_MRR_ATTR_CLNINFO AS A
	JOIN DW.dbo.DW_MRR_FRM_CLNINFO AS B
	ON A.FRMCLN_KEY = B.FRMCLN_KEY
WHERE A.ATTR_KEY IN ('5128508') and B.PTNT_NO in (select distinct patno from BYUN_SOURCE_DATA.dbo.ACPATBAT)
			AND A.NMRC_DATA IS NOT NULL

UNION ALL

--3. BP, Systolic
SELECT
	'MRR_ATTR_CLNINFO' AS TABLENM
	,A.FRMCLN_KEY
	,B.PTNT_NO
	,B.MED_DATE
	,B.MED_TIME
	,B.FRM_DT
	,B.MED_DEPT
	,B.MED_DR
	,B.CLN_TYPE -- PATFG ��� (I, O, ..)
	,'SYSTOLICBP' AS ORDCODE
	,'SYSTOLICBP' AS SPCCODE
	,'mm[Hg]' AS UNIT
	,A.FRM_KEY
	,A.MRIT_KEY
	,A.ATTR_KEY
	,A.INS_TYPE
	,A.NMRC_DATA
	,A.DT_DATA
	,A.TXT_DATA
	,CASE WHEN ISNUMERIC(A.NMRC_DATA)=1 THEN REPLACE(REPLACE(REPLACE(A.NMRC_DATA,' ',''),',',''),'+','')
				ELSE NULL
	 END AS VALUE_AS_NUMBER
	,B.VLD_GB
FROM DW.dbo.DW_MRR_ATTR_CLNINFO AS A
	JOIN DW.dbo.DW_MRR_FRM_CLNINFO AS B
	ON A.FRMCLN_KEY = B.FRMCLN_KEY
WHERE A.ATTR_KEY IN ('6171209','5151812','6161169','5141427') and B.PTNT_NO in (select distinct patno from BYUN_SOURCE_DATA.dbo.ACPATBAT)
			AND A.NMRC_DATA IS NOT NULL

UNION ALL

--4. BP, Diastolic
SELECT
	'MRR_ATTR_CLNINFO' AS TABLENM
	,A.FRMCLN_KEY
	,B.PTNT_NO
	,B.MED_DATE
	,B.MED_TIME
	,B.FRM_DT
	,B.MED_DEPT
	,B.MED_DR
	,B.CLN_TYPE -- PATFG ��� (I, O, ..)
	,'DIASTOLICBP' AS ORDCODE
	,'DIASTOLICBP' AS SPCCODE
	,'mm[Hg]' AS UNIT
	,A.FRM_KEY
	,A.MRIT_KEY
	,A.ATTR_KEY
	,A.INS_TYPE
	,A.NMRC_DATA
	,A.DT_DATA
	,A.TXT_DATA
	,B.VLD_GB
	,CASE WHEN ISNUMERIC(A.NMRC_DATA)=1 THEN REPLACE(REPLACE(REPLACE(A.NMRC_DATA,' ',''),',',''),'+','')
				ELSE NULL
	 END AS VALUE_AS_NUMBER
FROM DW.dbo.DW_MRR_ATTR_CLNINFO AS A
	JOIN DW.dbo.DW_MRR_FRM_CLNINFO AS B
	ON A.FRMCLN_KEY = B.FRMCLN_KEY
WHERE A.ATTR_KEY IN ('6171210','5151813','6161170','5141428') and B.PTNT_NO in (select distinct patno from BYUN_SOURCE_DATA.dbo.ACPATBAT)
			AND A.NMRC_DATA IS NOT NULL
) S
--

SELECT ROW_NUMBER() over(order by newid()) as UID, *
 INTO BYUN_SOURCE_DATA.dbo.meta_measurement
FROM
(
-- pre_meta_measurement_I
SELECT A.TABLENM
	,A.PATNO collate Korean_Wansung_CI_AS AS PATNO
	,A.ORDSEQNO
	,A.ORDDATE
	,A.MEDDATE		--��������
	,NULL AS ADMTIME	--�����Ͻ�
	,A.SPCDATE		--��ü����
	,A.BLDGATTIME	--ä���Ͻ�(���ڵ�����Ͻ�)
	,A.ACPTTIME		--�����Ͻ�
	,A.EXECTIME		--�ǽ��Ͻ�
	,A.READTIME		--�ǵ��Ͻ�
	,A.REPTTIME		--�����Ͻ�
	,A.VERIFYTM	--���Ȯ���Ͻ�
	,NULL AS RECDATE
	,NULL AS RECDATETIME
	,CONVERT(DATE, COALESCE(A.BLDGATTIME, A.ACPTTIME, A.SPCDATE, A.ORDDATE),23)
	 AS MEASUREMENT_DATE
	,COALESCE(A.BLDGATTIME, A.ACPTTIME)
	 AS MEASUREMENT_DATETIME
	,NULL AS FRMCLN_KEY
	,NULL AS FRM_KEY
	,NULL AS MRIT_KEY
	,NULL AS ATTR_KEY
	,A.EXAMCODE collate Korean_Wansung_CI_AS AS EXAMCODE
	,A.ORDCODE collate Korean_Wansung_CI_AS AS ORDCODE
	,A.EXAMCODE collate Korean_Wansung_CI_AS AS MAINCODE
	,A.SPCCODE collate Korean_Wansung_CI_AS AS SPCCODE
	,A.SPCCODE collate Korean_Wansung_CI_AS AS SUBCODE
	,A.ORDCLSTYP collate Korean_Wansung_CI_AS AS ORDCLSTYP
	,A.SLIPCODE collate Korean_Wansung_CI_AS AS SLIPCODE
	,A.EXAMTYP collate Korean_Wansung_CI_AS	AS EXAMTYP	--�˻�з�
	,5001 AS MEASUREMENT_TYPE_CONCEPT_ID--Test ordered through EHR
	,NULL AS INS_TYPE
	,A.RSLTTYPE collate Korean_Wansung_CI_AS AS RSLTTYPE	--�������
	,A.RSLTCODE collate Korean_Wansung_CI_AS AS RSLTCODE	--����ڵ�
	,A.RSLTNUM collate Korean_Wansung_CI_AS	AS RSLTNUM	--���(��ġ)
	,A.RSLTTEXT collate Korean_Wansung_CI_AS AS RSLTTEXT	--���(TEXT)
	,A.CONCTEXT collate Korean_Wansung_CI_AS AS CONCTEXT	--���
	,A.OPERATOR_CONCEPT_ID
	,CASE WHEN ISNUMERIC(A.VALUE_AS_NUMBER)=1
						THEN CONVERT(FLOAT,REPLACE(REPLACE(REPLACE(A.VALUE_AS_NUMBER,' ',''),',',''),'+',''))
				ELSE NULL
	 END AS VALUE_AS_NUMBER
	,A.VALUE_AS_CONCEPT_ID
	,COALESCE(A.RSLTNUM,A.RSLTTEXT) collate Korean_Wansung_CI_AS AS VALUE_SOURCE_VALUE
	,A.UNIT collate Korean_Wansung_CI_AS AS UNIT			--�˻�������
	,A.NORMALFG collate Korean_Wansung_CI_AS AS NORMALFG	--���󱸺�
	,A.NORMMAXVAL collate Korean_Wansung_CI_AS AS NORMMAXVAL	--����ġ(��)
	,A.NORMMINVAL collate Korean_Wansung_CI_AS AS NORMMINVAL	--����ġ(��)
	,A.ORDDR collate Korean_Wansung_CI_AS AS ORDDR
	,A.CHADR collate Korean_Wansung_CI_AS AS CHADR
	,A.EXECDR collate Korean_Wansung_CI_AS AS EXECDR
	,NULL AS REGID
	,NULL AS EDITID
	,NULL AS MED_DR
	,A.ORDDR collate Korean_Wansung_CI_AS AS PROVIDER_ID
	,NULL AS MED_DEPT
	,A.PATFG collate Korean_Wansung_CI_AS AS PATFG
	,A.PRNACTYN collate Korean_Wansung_CI_AS AS PRNACTYN
	,A.DCYN collate Korean_Wansung_CI_AS AS DCYN
	,A.MKFG collate Korean_Wansung_CI_AS AS MKFG
	,NULL AS VLD_GB
	,CASE WHEN A.DCYN = 'Y' THEN  'N'
        WHEN A.MKFG NOT IN ('N', 'P') THEN  'N'
        ELSE 'Y'
	 END AS INCLUDE_YN1
	,CASE WHEN CONVERT(VARCHAR, COALESCE(A.BLDGATTIME, A.ACPTTIME, A.SPCDATE, A.ORDDATE),23) > CONVERT(VARCHAR, C.death_dt,23) THEN 'N'
	 			ELSE 'Y'
	 END AS INCLUDE_YN2
FROM BYUN_SOURCE_DATA.dbo.pre_meta_measurement_I A
		JOIN Byun_meta_1M.dbo.Dev_person B
		ON A.PATNO = B.PATNO
		LEFT JOIN (SELECT * FROM Byun_meta_1M.dbo.Dev_death) C
		ON A.PATNO = C.PATNO
WHERE	CONVERT(VARCHAR, COALESCE(A.BLDGATTIME, A.ACPTTIME, A.SPCDATE, A.ORDDATE),23) >= CONVERT(VARCHAR, B.Btdt,23)

UNION ALL

-- pre_meta_measurement_II
SELECT A.TABLENM
	,A.PATNO collate Korean_Wansung_CI_AS AS PATNO
	,NULL AS ORDSEQNO
	,NULL AS ORDDATE
	,NULL AS MEDDATE		--��������
	,A.ADMTIME	--�����Ͻ�
	,NULL AS SPCDATE		--��ü����
	,NULL AS BLDGATTIME	--ä���Ͻ�(���ڵ�����Ͻ�)
	,NULL AS ACPTTIME		--�����Ͻ�
	,NULL AS EXECTIME		--�ǽ��Ͻ�
	,NULL AS READTIME		--�ǵ��Ͻ�
	,NULL AS REPTTIME		--�����Ͻ�
	,NULL AS VERIFYTM	--���Ȯ���Ͻ�
	,NULL AS RECDATE
	,A.RECTIME AS RECDATETIME
	,CONVERT(VARCHAR, A.RECTIME,23) AS MEASUREMENT_DATE
	,A.RECTIME AS MEASUREMENT_DATETIME
	,NULL AS FRMCLN_KEY
	,NULL AS FRM_KEY
	,NULL AS MRIT_KEY
	,NULL AS ATTR_KEY
	,NULL AS EXAMCODE
	,A.ORDCODE
	,A.ORDCODE AS MAINCODE
	,A.SPCCODE
	,A.SPCCODE AS SUBCODE
	,NULL AS ORDCLSTYP
	,NULL AS SLIPCODE
	,NULL AS EXAMTYP		--�˻�з�
	,44818701 AS MEASUREMENT_TYPE_CONCEPT_ID--From physical examination
	,NULL AS INS_TYPE
	,NULL AS RSLTTYPE	--�������
	,NULL AS RSLTCODE	--����ڵ�
	,NULL AS RSLTNUM		--���(��ġ)
	,NULL AS RSLTTEXT	--���(TEXT)
	,NULL AS CONCTEXT	--���
	,NULL AS OPERATOR_CONCEPT_ID
	,CONVERT(FLOAT, REPLACE(REPLACE(REPLACE(A.VALUE_AS_NUMBER,' ',''),',',''),'+','')) AS VALUE_AS_NUMBER
	,NULL AS VALUE_AS_CONCEPT_ID
	,CONVERT(VARCHAR(50),A.VALUE_AS_NUMBER) AS VALUE_SOURCE_VALUE
	,A.UNIT			--�˻�������
	,NULL AS NORMALFG	--���󱸺�
	,NULL AS NORMMAXVAL	--����ġ(��)
	,NULL AS NORMMINVAL	--����ġ(��)
	,NULL AS ORDDR
	,NULL AS CHADR
	,NULL AS EXECDR
	,A.REGID collate Korean_Wansung_CI_AS AS REGID
	,A.EDITID collate Korean_Wansung_CI_AS AS EDITID
	,NULL AS MED_DR
	,COALESCE(A.REGID, A.EDITID) collate Korean_Wansung_CI_AS AS PROVIDER_ID
	,NULL AS MED_DEPT
	,NULL AS PATFG
	,NULL AS PRNACTYN
	,NULL AS DCYN
	,NULL AS MKFG
	,NULL AS VLD_GB
	,'Y' AS INCLUDE_YN1
	,CASE WHEN CONVERT(VARCHAR, A.RECTIME,23) > CONVERT(VARCHAR, C.death_dt,23) THEN 'N'
	 			ELSE 'Y'
	 END AS INCLUDE_YN2
FROM BYUN_SOURCE_DATA.dbo.pre_meta_measurement_II A
		JOIN Byun_meta_1M.dbo.Dev_person B
		ON A.PATNO = B.PATNO
		LEFT JOIN (SELECT * FROM Byun_meta_1M.dbo.Dev_death) C
		ON A.PATNO = C.PATNO
WHERE	CONVERT(VARCHAR, A.RECTIME,23) >= CONVERT(VARCHAR, B.Btdt,23)
/*
UNION ALL

-- pre_meta_measurement_III
SELECT A.TABLENM
	,CONVERT(VARCHAR(10),A.PATNO) AS PATNO
	,NULL AS ORDSEQNO
	,NULL AS ORDDATE
	,NULL AS MEDDATE		--��������
	,NULL AS ADMTIME	--�����Ͻ�
	,NULL AS SPCDATE		--��ü����
	,NULL AS BLDGATTIME	--ä���Ͻ�(���ڵ�����Ͻ�)
	,NULL AS ACPTTIME		--�����Ͻ�
	,NULL AS EXECTIME		--�ǽ��Ͻ�
	,NULL AS READTIME		--�ǵ��Ͻ�
	,NULL AS REPTTIME		--�����Ͻ�
	,NULL AS VERIFYTM	--���Ȯ���Ͻ�
	,A.RECDATE
	,A.RECDATETIME
	,CONVERT(VARCHAR, A.RECDATE,23) AS MEASUREMENT_DATE
	,A.RECDATETIME AS MEASUREMENT_DATETIME
	,NULL AS FRMCLN_KEY
	,NULL AS FRM_KEY
	,NULL AS MRIT_KEY
	,NULL AS ATTR_KEY
	,NULL AS EXAMCODE
	,A.ORDCODE
	,A.ORDCODE AS MAINCODE
	,A.SPCCODE
	,A.SPCCODE AS SUBCODE
	,NULL AS ORDCLSTYP
	,NULL AS SLIPCODE
	,NULL AS EXAMTYP		--�˻�з�
	,44818701 AS MEASUREMENT_TYPE_CONCEPT_ID--From physical examination
	,NULL AS INS_TYPE
	,NULL AS RSLTTYPE	--�������
	,NULL AS RSLTCODE	--����ڵ�
	,NULL AS RSLTNUM		--���(��ġ)
	,NULL AS RSLTTEXT	--���(TEXT)
	,NULL AS CONCTEXT	--���
	,NULL AS OPERATOR_CONCEPT_ID
	,CASE WHEN ISNUMERIC(A.VALUE_AS_NUMBER)=1
						THEN CONVERT(FLOAT,REPLACE(REPLACE(REPLACE(A.VALUE_AS_NUMBER,' ',''),',',''),'+',''))
				ELSE NULL
	 END AS VALUE_AS_NUMBER
	,NULL AS VALUE_AS_CONCEPT_ID
	,A.VALUE_SOURCE_VALUE
	,A.UNIT			--�˻�������
	,NULL AS NORMALFG	--���󱸺�
	,NULL AS NORMMAXVAL	--����ġ(��)
	,NULL AS NORMMINVAL	--����ġ(��)
	,NULL AS ORDDR
	,NULL AS CHADR
	,NULL AS EXECDR
	,NULL AS REGID
	,NULL AS EDITID
	,NULL AS MED_DR
	,NULL AS PROVIDER_ID
	,NULL AS MED_DEPT
	,NULL AS PATFG
	,NULL AS PRNACTYN
	,NULL AS DCYN
	,NULL AS MKFG
	,NULL AS VLD_GB
	,'Y' AS INCLUDE_YN1
	,CASE WHEN A.RECDATE > CONVERT(VARCHAR, C.DIEDATE,23) THEN 'N'
	 			ELSE 'Y'
	 END AS INCLUDE_YN2
FROM Byun_origin_Rand_2M.dbo.pre_meta_measurement_III A
		JOIN Byun_meta_2M.dbo.Dev_person B
		ON CONVERT(VARCHAR(10),A.PATNO) = B.PATNO
		LEFT JOIN (SELECT * FROM Byun_meta_2M.dbo.Dev_death) C
		ON CONVERT(VARCHAR(10),A.PATNO) = C.PATNO
WHERE	CONVERT(VARCHAR, A.RECDATE,23) >= CONVERT(VARCHAR, B.Btdt,23)
*/
UNION ALL

-- pre_meta_measurement_IV
SELECT A.TABLENM
	,A.PTNT_NO collate Korean_Wansung_CI_AS AS PATNO
	,NULL AS ORDSEQNO
	,NULL AS ORDDATE
	,A.MED_DATE AS MEDDATE		--��������
	,CASE WHEN CONVERT(VARCHAR(20), A.MED_TIME) LIKE '%00:00:00.0000000' THEN NULL
	 			ELSE A.MED_TIME
	 END AS ADMTIME	--�����Ͻ�
	,NULL AS SPCDATE		--��ü����
	,NULL AS BLDGATTIME	--ä���Ͻ�(���ڵ�����Ͻ�)
	,NULL AS ACPTTIME		--�����Ͻ�
	,NULL AS EXECTIME		--�ǽ��Ͻ�
	,NULL AS READTIME		--�ǵ��Ͻ�
	,NULL AS REPTTIME		--�����Ͻ�
	,NULL AS VERIFYTM	--���Ȯ���Ͻ�
	,CONVERT(VARCHAR, A.FRM_DT, 23) AS RECDATE
	,CASE WHEN CONVERT(VARCHAR(20), A.FRM_DT) LIKE '%00:00:00.0000000' THEN NULL
	 			ELSE A.FRM_DT
	 END AS RECDATETIME
	,CONVERT(VARCHAR, (coalesce(A.FRM_DT, A.MED_DATE)),23) AS MEASUREMENT_DATE
	,CASE WHEN CONVERT(VARCHAR, (coalesce(A.FRM_DT, A.MED_DATE)),23) LIKE '%00:00:00.0000000' THEN NULL
	 			ELSE CONVERT(VARCHAR, (coalesce(A.FRM_DT, A.MED_DATE)),23)
	 END AS MEASUREMENT_DATETIME
	,A.FRMCLN_KEY
	,A.FRM_KEY
	,A.MRIT_KEY
	,A.ATTR_KEY
	,NULL AS EXAMCODE
	,A.ORDCODE
	,A.ORDCODE AS MAINCODE
	,A.SPCCODE
	,A.SPCCODE AS SUBCODE
	,NULL AS ORDCLSTYP
	,NULL AS SLIPCODE
	,NULL AS EXAMTYP		--�˻�з�
	,44818701 AS MEASUREMENT_TYPE_CONCEPT_ID--From physical examination
	,A.INS_TYPE collate Korean_Wansung_CI_AS AS INS_TYPE--EMR ����� ����
	,NULL AS RSLTTYPE	--�������
	,NULL AS RSLTCODE	--����ڵ�
	,CONVERT(VARCHAR(20),A.NMRC_DATA) AS RSLTNUM		--���(��ġ)
	,A.TXT_DATA collate Korean_Wansung_CI_AS AS RSLTTEXT	--���(TEXT)
	,NULL AS CONCTEXT	--���
	,NULL AS OPERATOR_CONCEPT_ID
	,CASE WHEN ISNUMERIC(A.VALUE_AS_NUMBER)=1
						THEN CONVERT(FLOAT,REPLACE(REPLACE(REPLACE(A.VALUE_AS_NUMBER,' ',''),',',''),'+',''))
				ELSE NULL
	 END AS VALUE_AS_NUMBER
	,NULL AS VALUE_AS_CONCEPT_ID
	,COALESCE(CONVERT(VARCHAR(50), A.NMRC_DATA), A.TXT_DATA) collate Korean_Wansung_CI_AS AS VALUE_SOURCE_VALUE
	,A.UNIT			--�˻�������
	,NULL AS NORMALFG	--���󱸺�
	,NULL AS NORMMAXVAL	--����ġ(��)
	,NULL AS NORMMINVAL	--����ġ(��)
	,NULL AS ORDDR
	,NULL AS CHADR
	,NULL AS EXECDR
	,NULL AS REGID
	,NULL AS EDITID
	,A.MED_DR collate Korean_Wansung_CI_AS AS MED_DR
	,A.MED_DR  collate Korean_Wansung_CI_AS AS PROVIDER_ID
	,A.MED_DEPT collate Korean_Wansung_CI_AS AS MED_DEPT
	,A.CLN_TYPE collate Korean_Wansung_CI_AS AS PATFG
	,NULL AS PRNACTYN
	,NULL AS DCYN
	,NULL AS MKFG
	,A.VLD_GB collate Korean_Wansung_CI_AS AS VLD_GB--��ȿ�� ���� (Y: ���, N: �ҿ�)
	,CASE WHEN VLD_GB = 'Y' THEN 'Y'
	 			ELSE 'N'
	 END AS INCLUDE_YN1
	,CASE WHEN CONVERT(VARCHAR, (coalesce(A.FRM_DT, A.MED_DATE)),23) > CONVERT(VARCHAR, C.death_dt,23) THEN 'N'
	 			ELSE 'Y'
	 END AS INCLUDE_YN2
FROM BYUN_SOURCE_DATA.dbo.pre_meta_measurement_IV A
		JOIN Byun_meta_1M.dbo.Dev_person B
		ON A.PTNT_NO = B.PATNO
		LEFT JOIN (SELECT * FROM Byun_meta_1M.dbo.Dev_death) C
		ON A.PTNT_NO = C.PATNO
WHERE	CONVERT(VARCHAR, (coalesce(A.FRM_DT, A.MED_DATE)),23) >= CONVERT(VARCHAR, B.Btdt,23)

) S

--> [2019-10-31 00:45:17] 11,209,993 rows affected in 41 m 28 s 532 ms
--> step 2
    SELECT
         M1.*
        ,case
            when M1.MEDDATE <= dp1.Btdt then 'Y'
            when M1.ORDDATE is not null and M1.ORDDATE <= dp1.Btdt then 'Y'
            when M1.acptdt is not null and M1.acptdt < dp1.Btdt then 'Y'
            when M1.execdt is not null and M1.execdt < dp1.Btdt then 'Y'
            when M1.verificationdt is not null and M1.verificationdt < dp1.Btdt then 'Y'
            else 'N'
         end as Brith_err
        ,case
            when M1.MEDDATE > dd1.death_dt then 'Y'
            when M1.ORDDATE is not null and M1.ORDDATE > dd1.death_dt then 'Y'
            when M1.acptdt is not null and M1.acptdt > dd1.death_dt then 'Y'
            when M1.execdt is not null and M1.execdt > dd1.death_dt then 'Y'
            when M1.verificationdt is not null and M1.verificationdt < dd1.death_dt then 'Y'
            else 'N'
         end as death_err
        ,case
            when M1.MEDDATE > M1.acptdt or M1.MEDDATE > M1.execdt or M1.MEDDATE > M1.verificationdt  then 'Y'
            when M1.ORDDATE > M1.acptdt or M1.ORDDATE > M1.execdt or M1.ORDDATE > M1.verificationdt or M1.ORDDATE > M1.MEDDATE then 'Y'
            when M1.acptdt > M1.execdt or M1.acptdt > M1.verificationdt then 'Y'
            when M1.execdt > M1.verificationdt then 'Y'
            else 'N'
          end as date_err
 into Byun_Meta_1M.dbo.Dev_measurment_step1
    FROM
      (select
           TABLENM as tbnm
          ,UID as uniq_no
					,ORDSEQNO as ordseqno
					,EXAMTYP as exam_gb
          ,PATNO as patno
          ,MED_DEPT as meddept
          ,PATFG as patfg
					,case
            when ADMTIME is not null then ADMTIME
            else MEDDATE
           end as MEDDATE
          ,ORDDATE as orddate
          ,ACPTTIME as acptdt
          ,EXECTIME as execdt
          ,VERIFYTM as verificationdt
					,null as other_dt
					,ORDCODE as ordcode
        	,EXAMCODE as examcd
         ,COALESCE(RSLTTYPE,INS_TYPE) as rslt_typ
         ,RSLTNUM as rsltnum
         ,RSLTTEXT as rslttxt
         ,UNIT as unit
         ,NORMMAXVAL as nr_up
         ,NORMMINVAL as nr_lw
         ,PROVIDER_ID as provider
				 ,MKFG as order_gb
         ,PRNACTYN as prn_yn
         , CASE WHEN DCYN = 'Y'  THEN  'Y'
             WHEN MKFG NOT IN ('N', 'P')  THEN  'Y'
            -- WHEN PRNACTYN = 'Y'  THEN  'Y'
             WHEN VLD_GB not in ('Y')  THEN  'Y'
             ELSE 'N'
            END AS include_yn
          ,case
            when EXECTIME is not null then cast(EXECTIME as date)
            when RECDATETIME is not null then cast(RECDATETIME as date)
            else cast(COALESCE(ADMTIME,MEDDATE) as date)
           end as MEASUREMENT_DT
         ,case
            when EXECTIME is not null then cast(EXECTIME as datetime)
            when RECDATETIME is not null then cast(RECDATETIME as datetime)
            else cast(COALESCE(ADMTIME,MEDDATE) as datetime)
           end as MEASUREMENT_DTTM
        from
         BYUN_SOURCE_DATA.dbo.meta_measurement) AS M1
   inner join (select PATNO, Btdt from BYUN_META_1M.dbo.Dev_person) as dp1
                on dp1.PATNO = M1.PATNO COLLATE Korean_Wansung_CI_AS
   left join (select PATNO, death_dt from BYUN_META_1M.dbo.Dev_death) as dd1
                on dd1.PATNO = M1.PATNO COLLATE Korean_Wansung_CI_AS
--> [2019-10-31 16:18:24] 9,829,082 rows affected in 7 s 831 ms
--> step 3
select *
into Byun_Meta_1M.dbo.dev_measurement
from
(select m1.* ,v1.uniq_no as visit_id   from
(select * from BYUN_META_1M.dbo.Dev_measurment_step1 where patfg in ('H','O')) as m1
left join (select
                    uniq_no,patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG = 'O' and tbnm in ('AOOPDLST')) as v1
                on v1.patno COLLATE Korean_Wansung_CI_AS = m1.PATNO
										 and v1.visit_date = m1.MEDDATE
										 and v1.med_dept COLLATE Korean_Wansung_CI_AS = m1.MEDDEPT
										 and v1.med_dr COLLATE Korean_Wansung_CI_AS  = m1.provider
union all
select m1.* ,v1.uniq_no as visit_id   from
(select * from BYUN_META_1M.dbo.Dev_measurment_step1 where patfg in ('E','I','D') ) as m1
left join (select
                    uniq_no, patno, visit_date, visit_time, dsch_date, dsch_time, med_dr, med_dept, patfg,cancel_yn
                   from BYUN_META_1M.dbo.Dev_visit
                    where PATFG in ('E','I','D') and tbnm in ('APIPDSLT')) as v1
                on v1.patno COLLATE Korean_Wansung_CI_AS  = m1.PATNO
									 and v1.visit_time = m1.MEDDATE
                   and v1.med_dept COLLATE Korean_Wansung_CI_AS = m1.MEDDEPT
									 and v1.patfg COLLATE Korean_Wansung_CI_AS = m1.patfg
union all
select m1.* ,v1.uniq_no as visit_id   from
(select * from BYUN_META_1M.dbo.Dev_measurment_step1 where patfg in ('G','M') ) as m1
left join (select patno, visit_date, visit_time, dsch_date, dsch_time,med_dr, med_dept, uniq_no,cancel_yn, case when patfg ='S' then 'M' else patfg end as patfg
                       from BYUN_META_1M.dbo.Dev_visit
                        where PATFG in ('G','S')) as v1
                on v1.patno COLLATE Korean_Wansung_CI_AS = m1.PATNO
									and (cast(v1.visit_date as date) = cast(m1.MEDDATE as date))
										 and v1.patfg COLLATE Korean_Wansung_CI_AS = m1.patfg
union all
select *, null as visit_id from BYUN_META_1M.dbo.Dev_measurment_step1  where patfg is null )v
--> [2019-10-31 16:50:23] 10,343,619 rows affected in 34 s 575 ms