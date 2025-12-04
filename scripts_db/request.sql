SELECT T1.drug_name, T1.prescriptions_count
FROM insight_region_drug_choice AS T1
WHERE T1.region ILIKE '%Центральный%' AND T1.disease_group ILIKE '%органы дыхания%'
ORDER BY T1.prescriptions_count DESC
LIMIT 5;