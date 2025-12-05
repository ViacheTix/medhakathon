SELECT p.id_пациента, d.торговое_название
FROM prescriptions pr
JOIN drugs d ON pr.код_препарата = d.код_препарата
JOIN patients p ON pr.id_пациента = p.id_пациента
WHERE (pr.id_пациента, DATE(pr.дата_рецепта)) IN (
  SELECT id_пациента, DATE(дата_рецепта)
  FROM prescriptions
  GROUP BY id_пациента, DATE(дата_рецепта)
  HAVING COUNT(DISTINCT код_препарата) >= 3
)
LIMIT 1;
SELECT DISTINCT p.район_проживания
FROM patients p
JOIN prescriptions pr ON p.id_пациента = pr.id_пациента
JOIN diagnoses d ON pr.код_диагноза = d.код_мкб
WHERE EXTRACT(YEAR FROM pr.дата_рецепта) = 2024
AND (d.название_диагноза ILIKE '%ОРВИ%' OR d.класс_заболевания ILIKE '%ОРВИ%');
