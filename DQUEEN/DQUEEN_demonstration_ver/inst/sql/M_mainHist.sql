select
   category
   ,stratum1
   ,count(*) as count_val

from
(select dm1.*, dr1.stratum1 from @resultDB.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from @resultDB.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id not in (31,53,30,39,1,2,3,20)
union all
select distinct  dm1.*, 'Dev_measurement'stratum1 from @resultDB.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from @resultDB.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  30
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from @resultDB.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from @resultDB.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  31
union all
select distinct  dm1.*, 'Dev_visit_detail'stratum1 from @resultDB.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from @resultDB.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  53
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from @resultDB.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from @resultDB.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  54
union all
select distinct dm1.*, 'Dev_note' as stratum1 from @resultDB.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from @resultDB.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id = 20)v
group by category,stratum1
order by category, stratum1;