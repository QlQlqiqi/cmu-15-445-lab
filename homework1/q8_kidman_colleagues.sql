SELECT DISTINCT(name)
from crew,
  people
WHERE title_id in (
    SELECT title_id
    FROM people,
      crew
    WHERE name = "Nicole Kidman"
      AND people.person_id = crew.person_id
  )
  and (
    category = "actor"
    or category = "actress"
  )
  and crew.person_id = people.person_id
ORDER BY name;