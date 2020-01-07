  select
			ROW_NUMBER() over(order by newid()) AS uniq_no
      ,op2.*
      ,p1.Btdt
      ,d1.death_dt
     ,case
        when op2.min_dt < Btdt then 'Y'
        when op2.min_dt > d1.death_dt then 'Y'
        when op2.max_dt > d1.death_dt then 'Y'
        else 'N'
      end as date_Err
     ,op2.min_dt as op_stdt
     ,COALESCE(d1.death_dt, op2.max_dt) as op_endt
  into Byun_meta_1M.dbo.Dev_period
      from
      (select
           op1.PATNO
          ,min(op1.mindt) as min_dt
          ,max(op1.max_dt) as max_dt
      from
        ( select PATNO, min(MEDDATE) as mindt, COALESCE(max(dschdate),max(MEDDATE)) as max_dt
          from Byun_meta_1M.dbo.Dev_Diagnosis
            where convert(VARCHAR, MEDDATE ,23) between '1994-04-01' and '2018-01-01' and  convert(VARCHAR, dschdate ,23) < '2018-01-01'
          group by PATNO
          union all
          select PATNO, min(orddate) as min_dt, max(orddate) as max_dt
          from Byun_meta_1M.dbo.Dev_device
            where convert(VARCHAR, orddate ,23) between '1994-04-01' and '2018-01-01'
          group by PATNO
          union all
          select PATNO, min(ORDDATE) as min_dt, MAX(DATEADD(day,day,ORDDATE)) as max_dt
          from Byun_meta_1M.dbo.Dev_Drug
              where convert(VARCHAR, ORDDATE ,23) between '1994-04-01' and '2018-01-01' and  convert(VARCHAR, DATEADD(day,day,ORDDATE) ,23) < '2019-01-01'
          group by PATNO
          union all
          select PATNO, min(MEASUREMENT_DT) as min_dt, MAX(MEASUREMENT_DT) as max_dt
          from Byun_meta_1M.dbo.dev_measurement
               where convert(VARCHAR, MEASUREMENT_DT ,23) between '1994-04-01' and '2018-01-01'
          group by PATNO COLLATE Korean_Wansung_CI_AS
          union all
          select patno, min(frmdt) as min_dt, MAX(frmdt) as max_dt
          from Byun_meta_1M.dbo.dev_clinicalnote1
              where convert(VARCHAR, frmdt ,23) between '1994-04-01' and '2018-01-01'
          group by patno
          union all
          select patno as PATNO, min(frmdt) as min_dt, MAX(frmdt)
          from Byun_meta_1M.dbo.dev_observation
              where convert(VARCHAR, frmdt ,23) between '1994-04-01' and '2018-01-01'
          group by PATNO
          union all
          select PATNO, MIN(Procedure_date) as min_dt, MAX(Procedure_date) AS max_dt
          from Byun_meta_1M.dbo.Dev_procedure
              where convert(VARCHAR, Procedure_date ,23) between '1994-04-01' and '2018-01-01'
          group by PATNO
          union all
          select patno as patno, min(coldt) as min_dt, max(coldt) as max_dt
          from Byun_meta_1M.dbo.Dev_specimen
              where convert(VARCHAR, coldt ,23) between '1994-04-01' and '2018-01-01'
          group by patno
          union all
          select PATNO, min(visit_date) min_dt, max(visit_date) as max_dt
          from Byun_meta_1M.dbo.Dev_visit
            where convert(VARCHAR, visit_date ,23) between '1994-04-01' and '2018-01-01'
          group by PATNO  ) as op1
          group by PATNO ) as op2
          inner join (select PATNO, Btdt from Byun_meta_1M.dbo.Dev_person) as p1
           on  p1.PATNO = op2.PATNO
          left join (select PATNO, death_dt from Byun_meta_1M.dbo.Dev_death) as d1
          on d1.PATNO = op2.PATNO
-->[2019-10-31 18:29:47] 84565 rows affected in 4 s 172 ms
