with a as (
    select * from lineitem
)
select
  l_returnflag,
  l_linestatus,
  count(*) as count_order
from
  lineitem b join a
on b.l_returnflag = abc.l_returnflag
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;