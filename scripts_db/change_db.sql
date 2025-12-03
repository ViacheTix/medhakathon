DELETE FROM insight_cost_by_disease;

-- 1. Определяем базовые данные: пациент, диагноз, группа болезни, стоимость
WITH base AS (
    SELECT
        pr.id_пациента,
        pr.код_диагноза,
        d.класс_заболевания AS disease_group,
        pr.код_препарата, -- Добавлен для удобства в дальнейшем
        dr.стоимость AS cost
    FROM prescriptions pr
    JOIN drugs dr ON pr.код_препарата = dr.код_препарата
    JOIN diagnoses d ON pr.код_диагноза = d.код_мкб
),

-- 2. Агрегация по группе заболеваний (Disease Group)
agg AS (
    SELECT
        disease_group,
        SUM(cost) AS total_cost_group,
        COUNT(*) AS total_prescriptions
    FROM base
    GROUP BY disease_group
),

-- 3. Расчет стоимости на пациента
patient_costs AS (
    SELECT
        id_пациента,
        disease_group,
        SUM(cost) AS total_cost_per_patient_by_group
    FROM base
    GROUP BY id_пациента, disease_group
),

-- 4. Расчет средней стоимости на пациента для каждой группы заболеваний
avg_patient_cost AS (
    SELECT
        disease_group,
        AVG(total_cost_per_patient_by_group) AS avg_cost_per_patient
    FROM patient_costs
    GROUP BY disease_group
),

-- 5. Определение топ-3 самых дорогих препаратов для каждой группы заболеваний (ИСПРАВЛЕНО)
top_ranked_drugs AS (
    SELECT DISTINCT
        b.disease_group,
        dr."Торговое название",
        -- Присваиваем ранг каждому уникальному препарату внутри группы заболеваний
        ROW_NUMBER() OVER(
            PARTITION BY b.disease_group 
            ORDER BY dr.стоимость DESC
        ) AS rn
    FROM base b
    JOIN drugs dr ON b.код_препарата = dr.код_препарата
),
top_drugs AS (
    SELECT
        disease_group,
        -- Собираем в массив только те, у которых ранг 1, 2 или 3
        ARRAY_TO_STRING(
            ARRAY_AGG("Торговое название" ORDER BY rn ASC), ', '
        ) AS top_expensive_drugs
    FROM top_ranked_drugs
    WHERE rn <= 3 -- Фильтруем по топ-3
    GROUP BY disease_group
)

-- Финальная вставка данных
INSERT INTO insight_cost_by_disease
SELECT
    a.disease_group,
    a.total_cost_group * 1.0 / a.total_prescriptions AS avg_cost_per_prescription,
    apc.avg_cost_per_patient, -- Средняя стоимость на пациента
    td.top_expensive_drugs,   -- Список самых дорогих препаратов
    CURRENT_TIMESTAMP AS date_updated
FROM agg a
JOIN avg_patient_cost apc ON a.disease_group = apc.disease_group
JOIN top_drugs td ON a.disease_group = td.disease_group;