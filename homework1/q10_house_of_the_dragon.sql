WITH t AS (
  select titles.primary_title as name,
    akas.title as dubbed
  from titles
    INNER JOIN akas on titles.title_id = akas.title_id
  where titles.primary_title = "House of the Dragon"
    AND titles.type = 'tvSeries'
  GROUP BY name,
    dubbed
  ORDER BY dubbed
),
seq as (
  SELECT row_number() OVER (
      ORDER BY t.name DESC
    ) as seq_num,
    dubbed
  FROM t
),
str AS (
  SELECT seq_num,
    dubbed
  FROM seq
  WHERE seq_num = 1
  UNION ALL
  SELECT seq.seq_num,
    str.dubbed || ', ' || seq.dubbed
  FROM seq
    INNER JOIN str on seq.seq_num = str.seq_num + 1
)
SELECT dubbed
from str
ORDER BY seq_num DESC
LIMIT 1;