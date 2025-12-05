WITH patient_drugs AS (
  SELECT id_пациента, дата_рецепта, код_препарата
  FROM prescriptions
)
SELECT id_пациента, дата_рецепта, COUNT(DISTINCT код_препарата) as num_drugs
FROM patient_drugs
GROUP BY id_пациента, дата_рецепта
HAVING COUNT(DISTINCT код_препарата) >= 3
LIMIT 1;