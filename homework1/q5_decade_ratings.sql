SELECT (premiered - premiered % 10) || "s" AS decade,
  round(AVG(rating), 2) AS avg_rating,
  MAX(rating),
  min(rating),
  count(*)
from titles as t,
  ratings as r
WHERE t.premiered is NOT NULL
  and t.title_id = r.title_id
GROUP BY decade
ORDER BY avg_rating DESC,
  decade;