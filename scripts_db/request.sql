SELECT COUNT(DISTINCT p.id_пациента) 
FROM prescriptions pr
JOIN patients p ON pr.id_пациента = p.id_пациента
JOIN diagnoses d ON pr.код_диагноза = d.код_мкб
WHERE EXTRACT(MONTH FROM pr.дата_рецепта) = 10 
  AND EXTRACT(YEAR FROM pr.дата_рецепта) = 2024
  AND (d.название_диагноза ILIKE '%ОРВИ%' OR d.класс_заболевания ILIKE '%ОРВИ%');