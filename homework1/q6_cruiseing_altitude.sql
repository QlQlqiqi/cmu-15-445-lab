SELECT primary_title,
  votes
from ratings as r,
  titles as t
WHERE t.title_id = r.title_id
  AND t.title_id in (
    SELECT title_id
    from crew
    WHERE person_id IN(
        SELECT person_id
        from people
        WHERE born = 1962
          AND name LIKE "%Cruise%"
      )
  )
ORDER BY votes DESC
LIMIT 10;