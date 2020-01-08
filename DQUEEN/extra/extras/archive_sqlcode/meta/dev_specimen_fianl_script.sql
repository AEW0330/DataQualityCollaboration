select
			 s1.*
			,tm1.QTY
			,tm1.UNIT
			,tm2.BLDQTY
into Byun_meta_1M.dbo.Dev_specimen_step1
from
(SELECT
		 ROW_NUMBER() over(order by newid()) as uid
		,null as visit_no
		,COL.PATNO as patient_id  ,
		COL.COLLTIME as collect_dt ,
		COL.ACPTTIME as acpt_dt,
		COL.MEDDATE as medical_dt ,
		SPA.PATFG as patfg,
		COL.ORDCODE as order_cd
		,SPA.PATHFG + right(SPA.PATHYEAR, 2) + '-' + RIGHT('00000' + CONVERT( NVARCHAR, SPA.PATHNO), 5) as sample_id ,
		COL.SPCCODE as sample_cd  ,
		COL.MEDDEPT as medical_dept ,
		COL.ORDDR as order_DR ,
		COL.CHADR as charge_dr ,
		COL.CANCELTM as cancel_dt ,
		COL.SPECLOCT as sample_site_1  ,
		SPA.TISPOS as sample_site_2  ,
		null as sample_unit
		,SPA.TISWEIGHT as sample_qty_1 ,
		SPA.TISQTY	as sample_qty_2	,
		SPA.BLOCKCNT as sample_cnt_1  ,
		SPA.TISCNT	 as sample_cnt_2	,
		SPA.PROCSTAT	 as sample_st  ,
		DIA.TISSCODE_NM AS tissue_cd_nm
		,DIA.TISSCODE as tissue_cd
		,SPA.P03RMK	as tissue_text ,
		DIA.DIAGCODE as diagnosis_cd,
		SPA.PATFG as visit_gb 	,
		null as sample_rslt
		,COL.EXAMTYP  as examination_gb  	,
		SPA.REMARK	 as etc  ,
		null as cancel_yn
		,SPA.VERIFYTYPINGTM	,
		DIA.OPCODE
		,DIA.OPCODE_NM
		,COL.BARPRNTM	,
		COL.SPCDATE	,
		COL.SPCNO		,
		COL.SPCSEQ		,
		COL.ORDDATE	,
		COL.CANCELPS	,
		SPA.DEATHDATE	,
		SPA.STFG		,
		SPA.REGFG

		from (select cdt.ORDSEQNO,cdt.ORDCODE,cdt.SPECLOCT, cmt.* from BYUN_SOURCE_DATA.dbo.SPCOLLMT as cmt
				JOIN BYUN_SOURCE_DATA.dbo.SPCOLLDT as cdt
				on cdt.PATNO=cmt.PATNO
			WHERE  cdt.SPCDATE=cmt.SPCDATE  AND  cdt.SPCNO=cmt.SPCNO AND  cdt.SPCSEQ=cmt.SPCSEQ
			and cmt.CANCELTM is null AND COLLTIME IS NOT NULL) as COL
			 JOIN (SELECT  DT.ORDDATE, DT.ORDSEQNO, DT.ORDCODE, MT.*
						FROM BYUN_SOURCE_DATA.dbo.SPACPTMT AS MT
						 JOIN BYUN_SOURCE_DATA.dbo.SPACPTDT DT
							ON DT.PATNO = MT.PATNO
							WHERE DT.PATHFG = MT.PATHFG
							AND DT.PATHYEAR = MT.PATHYEAR
							AND DT.PATHNO = MT.PATHNO AND PROCSTAT = 'H' ) as SPA
			on COL.PATNO = SPA.PATNO
			left join BYUN_SOURCE_DATA.dbo.SPDIAGMT DIA
			on SPA.PATHFG + right(SPA.PATHYEAR, 2) + '-' + RIGHT('00000' + CONVERT( NVARCHAR, SPA.PATHNO), 5)  = DIA.PATHFG + right(DIA.PATHYEAR, 2) + '-' + RIGHT('00000' + CONVERT( NVARCHAR, DIA.PATHNO), 5)
			where COL.ORDDATE = SPA.ORDDATE
			and COL.ORDSEQNO = SPA.ORDSEQNO
			and COL.ORDCODE = SPA.ORDCODE) as s1
	left join
		(select
					 s1.SPCCODE  , --검체코드
					s1.SPCNAME , --검체명
					s1.ABBRNAME, --약어명
					s1.SPCTYP , --검체종류(SL013)
					s1.TUBECODE , --용기코드
					s2.TUBENAME
					,s2.QTY
					,s2.UNIT
					,s1.LABELCNT , --Label 개수 -->
					s1.APPLTIME, --적용일시
					s1.EXPTIME--폐기일시
				from DW.dbo.DW_SLSPCMCT as s1
					left join (select TUBECODE,TUBENAME,QTY,UNIT,APPLTIME from Byun_origin_Rand_2M.dbo.SLTUBECT) as s2
				on s1.TUBECODE = s2.TUBECODE) as tm1
		on s1.sample_cd = TM1.SPCCODE
	left join
		(
				select
						 ORDCODE , --처방코드
						EXAMTYP , -- 검사분류
						FULLNAME , -- 검사전체명
						BLDQTY , -- 혈액용량(혈액은행)
						BLDUNIT , -- 혈액단위(혈액은행)
						OLDORDCODE , -- 기존처방코드
						AVAIL , -- 유효숫자자리수
						APPLTIME , -- 적용일시
						EXPTIME -- 폐기일시
				from DW.dbo.DW_SLORDRCT
					where BLDQTY is not null ) as tm2
			on s1.order_cd = tm2.ORDCODE;
--
select
	s1.*
 ,case when meddate >death_dt  then 'Y'
			 when orddate > death_dt then 'Y'
			 when orddate > death_dt then 'Y'
			 when coldt > death_dt   then 'Y '
	else 'N'
		end as death_err
 ,case when meddate <btdt  then 'Y'
			 when orddate < btdt then 'Y'
			 when orddate < btdt then 'Y'
			 when coldt < btdt   then 'Y '
	else 'N'
		end as birth_err
 ,case
		when cast(meddate as date) > cast(orddate as date) or meddate > acptdt or meddate> coldt then 'Y'
		when cast(orddate as date) > cast(acptdt as date) and orddate > coldt then 'Y'
	else 'N'
	end as date_err
into BYUN_META_1M.dbo.Dev_specimen
from
(select
'dev_specimen_step1' as tbnm
,uid as uniq_no
,examination_gb as execcd
,sample_id as spcno
,patient_id as patno
,patfg as patfg
,medical_dept as meddept
,medical_dt as meddate
,ORDDATE as orddate
,acpt_dt as acptdt
,collect_dt as coldt
,sample_cd as spc_cd
,sample_unit as unit
,sample_site_1 as samplesite
,sample_site_2
,sample_qty_1 as sample_qty
,sample_qty_2
,sample_cnt_1 as sample_cnt
,sample_cnt_2
,tissue_cd as tisscd
,tissue_cd_nm as tissnm
,tissue_text as tisstxt
,diagnosis_cd as diagcd
,OPCODE as opcd
,sample_rslt as result
,sample_st as com_gb
,cancel_yn as cancel_yn
,order_cd as ordcd
,order_DR as orddr
,case when cast(cancel_yn as varchar) = 'Y' then 'N'
			when cancel_dt is not null then 'N'
			when sample_st not in ('H') then 'N'
			else 'Y'
 end  as include_yn
from BYUN_META_1M.dbo.dev_specimen_step1) as s1
inner join
		(select patno, btdt from BYUN_META_1M.dbo.Dev_person) as p1
		on s1.patno = p1.patno
left join
		(select patno, death_dt from BYUN_META_1M.dbo.Dev_Death) as d1
		on s1.patno = d1.patno
--