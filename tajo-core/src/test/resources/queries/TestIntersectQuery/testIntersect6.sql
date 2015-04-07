SELECT
  count(*)
FROM (
  SELECT
    *
  FROM
    lineitem

  intersect

  SELECT
    *
  FROM
    lineitem
) T