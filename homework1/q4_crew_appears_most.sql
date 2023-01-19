SELECT name,
  appearance_num
FROM people as p,
  (
    SELECT count(*) as appearance_num,
      person_id
    from crew
    GROUP BY person_id
    ORDER BY appearance_num DESC
    LIMIT 20
  ) as c
WHERE c.person_id = p.person_id;