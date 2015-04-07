select
  count(*)
from (

  select
    count(*) as total
  from
    orders

  intersect all

  select
    count(*) as total
  from
    customer
) table1;