SELECT name,
  IFNULL(died, 2022) - born AS age
FROM people
WHERE born >= 1900
ORDER BY age DESC,name
LIMIT 20;