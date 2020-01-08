-- 기본데이터 생성 적재 필요 ?? 논의 필요함

SELECT
    case
        when TABLE_NAME = 'Dev_Condition' then 1
        when TABLE_NAME = 'Dev_Cost' then 2
        when TABLE_NAME = 'Dev_death' then 3
        when TABLE_NAME = 'Dev_Device2' then 4
        when TABLE_NAME = 'Dev_Drug' then 5
        when TABLE_NAME = 'Dev_measurment' then 6
        when TABLE_NAME = 'Dev_Note' then 7
        when TABLE_NAME = 'Dev_Observation' then 8
        when TABLE_NAME = 'Dev_observation_period' then 9
        when TABLE_NAME = 'Dev_ord_master' then 10
        when TABLE_NAME = 'Dev_payer_plan' then 11
        when TABLE_NAME = 'Dev_person' then 12
        when TABLE_NAME = 'Dev_procedure2' then 13
        when TABLE_NAME = 'Dev_provider' then 14
        when TABLE_NAME = 'Dev_siteinfo' then 15
        when TABLE_NAME = 'Dev_specimen2' then 16
        when TABLE_NAME = 'Dev_visit_detail' then 17
            else null
        end as tb_id
    , TABLE_NAME as tbnm
    , ORDINAL_POSITION as col_id
    , COLUMN_NAME as colnm
    , case
        when COLUMN_NAME like '%Uniq%' then 'Y'
        when TABLE_NAME = 'Dev_person' and COLUMN_NAME in ('PATNO','SEX', 'Btdt')  then 'Y'
        when TABLE_NAME = 'Dev_Condition' and COLUMN_NAME in ('PATNO','MEDDATE','SEQNO','DIAGCODE')  then 'Y' -- tbnm pk??
        when TABLE_NAME = 'Dev_Cost' and COLUMN_NAME in ('PATNO','RCPDATE','RCPSEQNO','SEQNO') then 'Y'
        when TABLE_NAME = 'Dev_Device2' and COLUMN_NAME in ('PATNO','DEVICE_STDT','ORDSEQNO','MEDDEPT') then 'Y'
        when TABLE_NAME = 'Dev_Drug' and COLUMN_NAME in ('PATNO','ORDDATE','ORDSEQNO','MEDDEPT') then 'Y' -- 데이터 변경 후, exec_seq
        when TABLE_NAME = 'Dev_Note' and COLUMN_NAME in ('PTNT_NO','FRMDT','FRMCLN_KEY') then 'Y' -- FRMCLN_KEY 별 SEQ 추가
        when TABLE_NAME = 'Dev_Observation' and COLUMN_NAME in ('PTNT_NO','FRM_DT','UID') then 'Y' -- 향 후 FRMCLN_KEY, SEQNO 추가
        when TABLE_NAME = 'Dev_death' and COLUMN_NAME in ('PATNO','DIEDATE','Cf_Type') then 'Y'
        when TABLE_NAME = 'Dev_measurment' and COLUMN_NAME in ('PATNO','MEASUREMENT_DTTM','ORDCODE','Typ') then 'Y'
        when TABLE_NAME = 'Dev_observation_period' and COLUMN_NAME in ('PATNO','opstdt','opendt') then 'Y'
        when TABLE_NAME = 'Dev_ord_master' and COLUMN_NAME in ('tbnm','local_cd','INSEDICODE') then 'Y'
        when TABLE_NAME = 'Dev_payer_plan' and COLUMN_NAME in ('PATNO','Suga_typ','ORDDATE') then 'Y' -- seq 수정 필요
        when TABLE_NAME = 'Dev_procedure2' and COLUMN_NAME in ('PATNO','ORDDATE','ORDSEQNO','Seq','typ') then 'Y'
        when TABLE_NAME = 'Dev_provider' and COLUMN_NAME in ('USERID','EMPNO','POSITION') then 'Y'
        when TABLE_NAME = 'Dev_siteinfo' and COLUMN_NAME in ('site_cd') then 'Y'
        when TABLE_NAME = 'Dev_specimen2' and COLUMN_NAME in ('patient_id','medical_dt','ORDDATE','sample_id') then 'Y'
        when TABLE_NAME = 'Dev_visit_detail' and COLUMN_NAME in ('patno','visit_date','dsch_date','patfg') then 'Y'
        else 'N'
      end as pk_yn
    , case
        when TABLE_NAME = 'Dev_person' and COLUMN_NAME in ('PATNO', 'Btdt')  then 'Y'
        when TABLE_NAME = 'Dev_Condition' and COLUMN_NAME in ('PATNO','MEDDATE','visit_time','MEDDEPT')  then 'Y' -- tbnm pk??
        when TABLE_NAME = 'Dev_Cost' and COLUMN_NAME in ('PATNO','ORDDATE','MEDDEPT','ORDSEQNO','ORDDR') then 'Y'
        when TABLE_NAME = 'Dev_Device2' and COLUMN_NAME in ('PATNO','DEVICE_STDT','ORDSEQNO','MEDDEPT','ORDDR') then 'Y'
        when TABLE_NAME = 'Dev_Drug' and COLUMN_NAME in ('PATNO','ORDDATE','ORDSEQNO','MEDDEPT','ORDDR') then 'Y' -- 데이터 변경 후, exec_seq
        when TABLE_NAME = 'Dev_Note' and COLUMN_NAME in ('PTNT_NO','FRMDT','FRMCLN_KEY','MED_DR') then 'Y' -- FRMCLN_KEY 별 SEQ 추가
        when TABLE_NAME = 'Dev_Observation' and COLUMN_NAME in ('PTNT_NO','FRM_DT','MED_DR') then 'Y' -- 향 후 FRMCLN_KEY, SEQNO 추가
        when TABLE_NAME = 'Dev_death' and COLUMN_NAME in ('PATNO') then 'Y'
        when TABLE_NAME = 'Dev_measurment' and COLUMN_NAME in ('PATNO','MEASUREMENT_DTTM','ORDDATE','ORDCODE','PROVIDER_ID') then 'Y'
        when TABLE_NAME = 'Dev_observation_period' and COLUMN_NAME in ('PATNO','opstdt','opendt') then 'Y'
        when TABLE_NAME = 'Dev_ord_master' and COLUMN_NAME in ('tbnm','local_cd') then 'Y'
        when TABLE_NAME = 'Dev_payer_plan' and COLUMN_NAME in ('PATNO','Suga_typ','ORDDATE') then 'Y' -- seq 수정 필요
        when TABLE_NAME = 'Dev_procedure2' and COLUMN_NAME in ('PATNO','ORDDATE','ORDSEQNO','ORDDR') then 'Y'
        when TABLE_NAME = 'Dev_provider' and COLUMN_NAME in ('USERID','DEPTCODE') then 'Y'
        when TABLE_NAME = 'Dev_siteinfo' and COLUMN_NAME in ('site_cd') then 'Y'
        when TABLE_NAME = 'Dev_specimen2' and COLUMN_NAME in ('patient_id','medical_dt','ORDDATE','sample_id') then 'Y'
        when TABLE_NAME = 'Dev_visit_detail' and COLUMN_NAME in ('patno','visit_date','dsch_date','patfg') then 'Y'
        else 'N'
      end as fk_yn
    , IS_NULLABLE
    , DATA_TYPE
    , case
        when DATA_TYPE like '%int%'  then 'int'
        when DATA_TYPE like '%char%' or DATA_TYPE like '%TEXT%' then 'char'
        when DATA_TYPE = 'numeric' or DATA_TYPE = 'flaot' or DATA_TYPE = 'demical' then 'nuemric'
        WHEN DATA_TYPE like '%datetime%' then 'datetime'
        WHEN DATA_TYPE = 'date' then 'date'
        else null
      end as Maps_to_TYPE
    , CHARACTER_MAXIMUM_LENGTH
    , NUMERIC_PRECISION  -- 정밀성
    , DATETIME_PRECISION
into @result_schema.dbo.metadb_info
        FROM Byun_meta_2M.INFORMATION_SCHEMA.COLUMNS
         where TABLE_NAME in (
                                 'Dev_Condition'
                                ,'Dev_Cost'
                                ,'Dev_death'
                                ,'Dev_Device2'
                                ,'Dev_Drug'
                                ,'Dev_measurment'
                                ,'Dev_Note'
                                ,'Dev_Observation'
                                ,'Dev_observation_period'
                                ,'Dev_ord_master'
                                ,'Dev_payer_plan'
                                ,'Dev_person'
                                ,'Dev_procedure2'
                                ,'Dev_provider'
                                ,'Dev_siteinfo'
                                ,'Dev_specimen2'
                                ,'Dev_visit_detail'
                                     );
