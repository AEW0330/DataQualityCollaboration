select
'M' as stage
,category
,count(*) as count_val
from @resultDB.dq_result_summary_main
where stage = '@type'
group by category;