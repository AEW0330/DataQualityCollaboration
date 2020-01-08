-- Dev_observationperiod

  select
       op2.*
      ,p1.Btdt
      ,d1.DIEDATE
     ,case
        when op2.min_dt < Btdt then 'Y'
        when op2.min_dt > d1.DIEDATE then 'Y'
        when op2.max_dt > d1.DIEDATE then 'Y'
        else 'N'
      end as ERR
     ,op2.min_dt as op_stdt
     ,COALESCE(d1.DIEDATE, op2.max_dt) as op_endt
    into Byun_meta_2M.dbo.Dev_observation_period
      from
      (select
           op1.PATNO
          ,min(op1.mindt) as min_dt
          ,max(op1.max_dt) as max_dt
      from
        ( select PATNO, min(MEDDATE) as mindt, COALESCE(max(dsch_date),max(MEDDATE)) as max_dt
          from Byun_meta_2M.dbo.Dev_Condition
            where convert(VARCHAR, MEDDATE ,23) between '1994-04-01' and '2018-01-01' and  convert(VARCHAR, dsch_date ,23) < '2018-01-01'
          group by PATNO
          union all
          select PATNO, min(Device_STDT) as min_dt, max(DEVICE_ENDT) as max_dt
          from Byun_meta_2M.dbo.Dev_Device2
            where convert(VARCHAR, Device_STDT ,23) between '1994-04-01' and '2018-01-01' and  convert(VARCHAR, DEVICE_ENDT ,23) < '2018-01-01'
          group by PATNO
          union all
          select PATNO, min(ORDDATE) as min_dt, MAX(DATEADD(day,R_DAY,ORDDATE)) as max_dt
          from Byun_meta_2M.dbo.Dev_Drug
              where convert(VARCHAR, ORDDATE ,23) between '1994-04-01' and '2018-01-01' and  convert(VARCHAR, DATEADD(day,R_DAY,ORDDATE) ,23) < '2019-01-01'
          group by PATNO
          union all
          select PATNO, min(MEASUREMENT_DT) as min_dt, MAX(MEASUREMENT_DT) as max_dt
          from Byun_meta_2M.dbo.Dev_measurment
               where convert(VARCHAR, MEASUREMENT_DT ,23) between '1994-04-01' and '2018-01-01'
          group by PATNO COLLATE Korean_Wansung_CI_AS
          union all
          select PTNT_NO as PATNO, min(FRM_DT) as min_dt, MAX(FRM_DT) as max_dt
          from Byun_meta_2M.dbo.Dev_Note
              where convert(VARCHAR, FRM_DT ,23) between '1994-04-01' and '2018-01-01'
          group by PTNT_NO
          union all
          select PTNT_NO as PATNO, min(FRM_DT) as min_dt, MAX(FRM_DT)
          from Byun_meta_2M.dbo.Dev_Observation
              where convert(VARCHAR, FRM_DT ,23) between '1994-04-01' and '2018-01-01'
          group by PTNT_NO
          union all
          select PATNO, MIN(Procedure_date) as min_dt, MAX(Procedure_date) AS max_dt
          from Byun_meta_2M.dbo.Dev_procedure2
              where convert(VARCHAR, Procedure_date ,23) between '1994-04-01' and '2018-01-01'
          group by PATNO
          union all
          select patient_id as patno, min(specimen_dt) as min_dt, max(specimen_dt) as max_dt
          from Byun_meta_2M.dbo.Dev_specimen2
              where convert(VARCHAR, specimen_dt ,23) between '1994-04-01' and '2018-01-01'
          group by patient_id
          union all
          select PATNO, min(visit_date) min_dt, max(visit_date) as max_dt
          from Byun_meta_2M.dbo.Dev_visit_detail
            where convert(VARCHAR, visit_date ,23) between '1994-04-01' and '2018-01-01'
          group by PATNO  ) as op1
          group by PATNO ) as op2
          inner join (select PATNO, Btdt from Byun_meta_2M.dbo.Dev_person) as p1
           on  p1.PATNO = op2.PATNO
          left join (select PATNO, DIEDATE from Byun_meta_2M.dbo.Dev_death) as d1
          on d1.PATNO = op2.PATNO

-- WHERE convert(VARCHAR, MDEVI.ORDER_DT ,23) between '1994-04-01' and '2017-12-31'