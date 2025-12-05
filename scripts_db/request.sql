SELECT 
    p.район_проживания, 
    COUNT(*) as cnt
FROM 
    prescriptions pr
JOIN 
    diagnoses d ON pr.код_диагноза = d.код_мкб
JOIN 
    patients p ON pr.id_пациента = p.id_пациента
WHERE 
    d.название_диагноза ILIKE '%нос%' 
    OR d.название_диагноза ILIKE '%ринит%' 
    OR d.название_диагноза ILIKE '%синусит%' 
    OR d.название_диагноза ILIKE '%рот%' 
    OR d.название_диагноза ILIKE '%стоматит%' 
    OR d.название_диагноза ILIKE '%гингивит%'
GROUP BY 
    p.район_проживания
ORDER BY 
    cnt DESC
LIMIT 1;