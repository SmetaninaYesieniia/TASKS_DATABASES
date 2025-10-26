USE intl_exchange_db;

SELECT
  e.enrollment_id,
  e.full_name,
  p.program_name,
  s.sponsor_name
FROM enrollments e
JOIN programs   p ON e.program_id = p.program_id
JOIN sponsors   s ON p.sponsor_id = s.sponsor_id
LIMIT 20;


SET @from_2023 = DATE('2023-01-01');
SET @to_2023   = DATE('2023-12-31');

# NON-OPTIMIZED код створений аішкою

DROP TEMPORARY TABLE IF EXISTS result_nonopt;

EXPLAIN ANALYZE
SELECT
  p.program_id,
  p.program_name,
  s.sponsor_name,
  COUNT(*) AS completed_students
FROM sponsors s
JOIN programs   p ON p.sponsor_id = s.sponsor_id
JOIN enrollments e ON e.program_id = p.program_id
WHERE DATE(e.start_date) >= DATE(@from_2023)
  AND DATE(e.end_date)   <= DATE(@to_2023)
  AND LOWER(CAST(e.status AS CHAR)) = 'completed'
  AND e.role LIKE '%student%'
  AND (p.field_domain = 'education' OR p.field_domain = 'research')
GROUP BY p.program_id, p.program_name, s.sponsor_name
ORDER BY completed_students DESC, p.program_id
LIMIT 100;

#Оптимізований код

USE intl_exchange_db;

EXPLAIN ANALYZE
WITH filtered_enrollments AS (
  SELECT e.program_id
  FROM enrollments e
  WHERE e.start_date >= '2023-01-01'
    AND e.end_date <= '2023-12-31'
    AND e.status = 'completed'
    AND e.role = 'student'
),
per_program AS (
  SELECT program_id, COUNT(*) AS completed_students
  FROM filtered_enrollments
  GROUP BY program_id
)
SELECT
  p.program_id,
  p.program_name,
  s.sponsor_name,
  pp.completed_students
FROM per_program pp
JOIN programs p ON p.program_id = pp.program_id
JOIN sponsors s ON s.sponsor_id = p.sponsor_id
WHERE p.field_domain IN ('education','research')
ORDER BY pp.completed_students DESC, p.program_id
LIMIT 100;
