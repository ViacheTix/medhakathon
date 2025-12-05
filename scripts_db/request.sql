SELECT 
    d.Торговое_название, 
    COUNT(p.id_пациента) AS count
FROM 
    prescriptions p
JOIN 
    drugs d ON p.код_препарата = d.код_препарата
JOIN 
    diagnoses di ON p.код_диагноза = di.код_мкб
JOIN 
    patients pa ON p.id_пациента = pa.id_пациента
WHERE 
    pa.район_проживания = 'Центральный'
    AND (di.название_диагноза ILIKE '%легкие%' 
         OR di.название_диагноза ILIKE '%органы дыхания%' 
         OR di.класс_заболевания ILIKE '%органы дыхания%')
GROUP BY 
    d.Торговое_название
ORDER BY 
    count DESC
LIMIT 5;