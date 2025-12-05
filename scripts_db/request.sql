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