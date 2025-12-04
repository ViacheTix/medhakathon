WITH disease_patients AS (
  SELECT p.район_проживания, COUNT(DISTINCT p.id_пациента) AS patients_count
  FROM patients p
  JOIN prescriptions pr ON p.id_пациента = pr.id_пациента
  JOIN diagnoses d ON pr.код_диагноза = d.код_мкб
  WHERE d.класс_заболевания ILIKE '%болезни системы кровообращения%'
  GROUP BY p.район_проживания
),
total_patients AS (
  SELECT район_проживания, COUNT(DISTINCT id_пациента) AS total_count
  FROM patients
  GROUP BY район_проживания
),
disease_ratio AS (
  SELECT dp.район_проживания, 
         ROUND(dp.patients_count * 1.0 / tp.total_count, 4) AS disease_ratio
  FROM disease_patients dp
  JOIN total_patients tp ON dp.район_проживания = tp.район_проживания
),
avg_disease_ratio AS (
  SELECT AVG(disease_ratio) AS avg_ratio
  FROM disease_ratio
)
SELECT dr.район_проживания, dr.disease_ratio
FROM disease_ratio dr
WHERE dr.disease_ratio > (SELECT avg_ratio FROM avg_disease_ratio)
ORDER BY dr.disease_ratio DESC
LIMIT 3;