select
  l_orderkey
from (
  select
    l_orderkey
  from
    lineitem l1

  intersect

  select
    l_orderkey
  from
    lineitem l2
) table1

order by
  l_orderkey;