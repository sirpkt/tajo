with a as (
    select * from lineitem
)
, b as (
    select * from lineitem
)
, c as ( with d as (select * from lineitem) ,e as (select * from lineitem) select * from lineitem)
, f as (
    select * from lineitem
)
select
  l_returnflag,
  l_linestatus,
  count(*) as count_order
from
  lineitem
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;