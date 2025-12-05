WITH district_patients AS (
  SELECT 
    p.район_проживания,
    COUNT(DISTINCT p.id_пациента) AS total_patients,
    SUM(CASE WHEN d.класс_заболевания ILIKE '%Болезни системы кровообращения%' THEN 1 ELSE 0 END) AS circulatory_patients
  FROM 
    patients p
  JOIN 
    prescriptions pr ON p.id_пациента = pr.id_пациента
  JOIN 
    diagnoses d ON pr.код_диагноза = d.код_мкб
  GROUP BY 
    p.район_проживания
),
avg_circulatory_rate AS (
  SELECT 
    SUM(circulatory_patients) / SUM(total_patients) AS avg_rate
  FROM 
    district_patients
)
SELECT 
  район_проживания,
  (circulatory_patients * 1.0 / total_patients) AS circulatory_rate
FROM 
  district_patients
WHERE 
  (circulatory_patients * 1.0 / total_patients) > (SELECT avg_rate FROM avg_circulatory_rate)
ORDER BY 
  circulatory_rate DESC
LIMIT 3;