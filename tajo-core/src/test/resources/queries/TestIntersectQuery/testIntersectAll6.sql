SELECT
  count(*)
FROM (
  SELECT
    *
  FROM
    lineitem

  intersect all

  SELECT
    *
  FROM
    lineitem
) T