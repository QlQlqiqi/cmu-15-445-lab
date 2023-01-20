with tmp AS (
  SELECT 1 AS a,
    3 AS b
),
flattened AS (
  SELECT *
  FROM tmp
  UNION ALL
  SELECT a,
    b
  FROM flattened
  LIMIT 10
), aaa as (
  SELECT row_number() over (
      ORDER BY a
    ) as seq,
    a,
    b
  from flattened
)
SELECT *
FROM aaa;
with test as (
  SELECT *
  from aaa
  WHERE seq = 1
  UNION ALL
  select seq + 1,
    a + 1,
    b + 2
  FROM test
  LIMIT 20
)
SELECT *
FROM test;