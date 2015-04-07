SELECT
  l_returnflag,
  l_linestatus

FROM (
  SELECT
    *
  FROM
    lineitem
  WHERE
    l_returnflag = 'R'

  intersect all

  SELECT
    *
  FROM
    lineitem
  WHERE
    l_returnflag = 'R'
) T
GROUP BY
  l_returnflag,
  l_linestatus;