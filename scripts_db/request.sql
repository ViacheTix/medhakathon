SELECT DISTINCT p.район_проживания
FROM patients p
JOIN prescriptions pr ON p.id_пациента = pr.id_пациента
JOIN diagnoses d ON pr.код_диагноза = d.код_мкб
WHERE EXTRACT(YEAR FROM pr.дата_рецепта) = 2024
AND (d.название_диагноза ILIKE '%ОРВИ%' OR d.класс_заболевания ILIKE '%ОРВИ%');