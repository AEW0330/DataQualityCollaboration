---
	select PATHFG + right(PATHYEAR, 2) + '-' + RIGHT('00000' + CONVERT( NVARCHAR, PATHNO), 5) as sequencing_id,
		 b.PERSON_ID as person_id, a.*
		into #SPDIAGMT
		from [SKT_CDM_���������� ].[dbo].[SPDIAGMT] a --(158�� ���� ������ ����)
			inner join [DW].[dbo].[PERSON_LIST] b
			on a.PATNO = b.PNO
select * from Byun_origin_Rand_2M.dbo.SPDIAGMT;
---
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
					left join (select TUBECODE,TUBENAME,QTY,UNIT,APPLTIME from DW.dbo.DW_SLTUBECT) as s2
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

-- [2019-05-29 19:11:52] 530,330 rows affected in 5 s 977 ms

--

-- 병리 검사
  select
   s1.*
  ,case
      when s1.sample_st = 'H' then 'N'
      else 'Y'
    end as del_yn
  ,case
      when s1.medical_dt <= dp1.Btdt then 'Y'
      when s1.ORDDATE is not null and s1.ORDDATE <= dp1.Btdt then 'Y'
      when s1.specimen_dt is not null and s1.specimen_dt < dp1.Btdt then 'Y'
      when s1.specimen_dttm is not null and s1.specimen_dttm < dp1.Btdt then 'Y'
      else 'N'
    end as Brith_err
  ,case
    when s1.medical_dt > dd1.DIEDATE then 'Y'
    when s1.ORDDATE is not null and s1.ORDDATE > dd1.DIEDATE then 'Y'
    when s1.specimen_dt is not null and s1.specimen_dt > dd1.DIEDATE then 'Y'
    when s1.specimen_dttm is not null and s1.specimen_dttm > dd1.DIEDATE then 'Y'
    else 'N'
  end as death_err
  ,case
    when s1.ORDDATE > s1.medical_dt then 'Y'
    --when (s1.visit_time is not null and s1.dsch_time is not null) and s1.visit_time > s1.dsch_time then 'Y'
    else 'N'
   end as date_err
into Byun_meta_2M.dbo.Dev_specimen2
  from
    (select
       uid
      ,patient_id
      ,patfg
      ,medical_dept
      ,medical_dt
      ,ORDDATE
      ,case
        when collect_dt is not null then cast(collect_dt as date)
        else SPCDATE
       end as specimen_dt
      ,case
        when collect_dt is not null then cast(collect_dt as datetime)
        else null
       end as specimen_dttm
      ,sample_id
      ,sample_cd
      ,sample_unit
      ,sample_site_1
      ,sample_site_2
      ,sample_qty_1
      ,sample_qty_2
      ,sample_cnt_1
      ,sample_cnt_2
      ,tissue_cd
      ,tissue_cd_nm
      ,tissue_text
      ,diagnosis_cd
      ,sample_st
      ,OPCODE
      ,OPCODE_NM

    from Byun_meta_2M.dbo.Dev_specimen) as s1
            inner join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as dp1
                on dp1.PATNO = s1.patient_id
            left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as dd1
                on dd1.PATNO = s1.patient_id

-- [2019-07-16 20:58:33] 530327 rows affected in 1 s 327 ms





