SELECT disease_group, 
       female_minus_male, 
       CASE 
           WHEN female_minus_male > 0 THEN 'женщины'
           WHEN female_minus_male < 0 THEN 'мужчины'
           ELSE 'равно'
       END AS кто_болеет_чаще
FROM insight_gender_disease
ORDER BY ABS(female_minus_male) DESC;