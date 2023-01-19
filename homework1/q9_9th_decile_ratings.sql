SELECT name,
  avg_rating
from (
    SELECT ntile(10) over (
        ORDER BY avg_rating,
          name DESC
      ) as rank,
      name,
      avg_rating
    from (
        SELECT round(avg(r.rating), 2) as avg_rating,
          name
        from titles as t,
          (
            SELECT name,
              title_id,
              people.person_id
            from crew,
              people
            WHERE crew.person_id = people.person_id
              AND born = 1955
          ) as p,
          ratings as r
        WHERE type = "movie"
          and t.title_id = p.title_id
          AND p.title_id = r.title_id
        GROUP BY p.person_id
      )
  )
WHERE rank = 9
ORDER BY avg_rating DESC,
  name;