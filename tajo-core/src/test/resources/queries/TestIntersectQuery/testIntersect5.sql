select
  count(*)
from (

  select
    count(*) as total
  from
    orders
  WHERE
    o_orderkey > 0

  intersect

  select
    count(*) as total
  from
    customer
  WHERE
    c_custkey > 0
) table1;