with a as (
    select * from lineitem2
)
, b as (
    select * from lineitem1
)
, c as ( with d as (select * from lineitem8) ,e as (select * from lineitem5) select * from lineitem7)
, f as (
    select * from lineitem9
)
select
  l_returnflag,
  l_linestatus,
  count(*) as count_order
from
  lineitem;