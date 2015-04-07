select
  total
from (

select
  count(*) as total
from
  orders

intersect

select
  count(*) as total
from
  customer
) table1

order by
  total;