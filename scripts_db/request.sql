SELECT region, AVG(avg_cost_per_patient) as avg_cost
FROM insight_cost_by_disease
WHERE disease_group ILIKE '%болезни нервной системы%'
GROUP BY region
ORDER BY avg_cost DESC
LIMIT 1;