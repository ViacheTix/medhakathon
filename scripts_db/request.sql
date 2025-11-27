-- request.sql
SELECT
    YEAR(p.дата_рецепта) AS год,
    COUNT(*) AS количество_случаев
FROM prescriptions p
JOIN diagnoses d ON p.код_диагноза = d.код_мкб
WHERE
    d.код_мкб LIKE 'J0%'
    OR d.код_мкб LIKE 'J1%'
    OR d.код_мкб = 'J20.9'  -- иногда пневмония/бронхит могут идти вместе
GROUP BY YEAR(p.дата_рецепта)
ORDER BY количество_случаев DESC
LIMIT 10;