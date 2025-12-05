SELECT 
    strftime(дата_рецепта, '%Y') as year,
    COUNT(*) as cnt
FROM 
    prescriptions
JOIN 
    diagnoses ON prescriptions.код_диагноза = diagnoses.код_мкб
WHERE 
    diagnoses.название_диагноза ILIKE '%карди%' 
    OR diagnoses.название_диагноза ILIKE '%инфаркт%'
    OR diagnoses.название_диагноза ILIKE '%сердце%'
GROUP BY 
    year
ORDER BY 
    year;