SELECT
  orderkey
FROM (
  SELECT
    l_orderkey as orderkey
  FROM
    lineitem

  intersect all

  SELECT
    l_orderkey as orderkey
  FROM
    lineitem
) T

order by
  orderkey;