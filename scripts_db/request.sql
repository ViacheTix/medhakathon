WITH base AS (
    SELECT
        d.класс_заболевания,
        p.пол,
        COUNT(DISTINCT pr.id_пациента) AS кол_пациентов
    FROM prescriptions pr
    JOIN patients p ON pr.id_пациента = p.id_пациента
    JOIN diagnoses d ON pr.код_диагноза = d.код_мкб
    GROUP BY 1,2
),

ranked AS (
    SELECT
        класс_заболевания,
        SUM(кол_пациентов) AS всего_пациентов
    FROM base
    GROUP BY 1
    ORDER BY всего_пациентов DESC
    LIMIT 20
),

final AS (
    SELECT
        r.класс_заболевания,

        -- мужчины
        COALESCE(MAX(CASE WHEN b.пол = 'М' THEN b.кол_пациентов END), 0) AS мужчины,

        -- женщины
        COALESCE(MAX(CASE WHEN b.пол = 'Ж' THEN b.кол_пациентов END), 0) AS женщины,

        -- разница
        COALESCE(MAX(CASE WHEN b.пол = 'Ж' THEN b.кол_пациентов END), 0)
            - COALESCE(MAX(CASE WHEN b.пол = 'М' THEN b.кол_пациентов END), 0)
            AS разница_жен_минус_муж,

        -- кто чаще
        CASE
            WHEN COALESCE(MAX(CASE WHEN b.пол = 'М' THEN b.кол_пациентов END), 0)
               > COALESCE(MAX(CASE WHEN b.пол = 'Ж' THEN b.кол_пациентов END), 0)
            THEN 'чаще у мужчин'
            WHEN COALESCE(MAX(CASE WHEN b.пол = 'М' THEN b.кол_пациентов END), 0)
               < COALESCE(MAX(CASE WHEN b.пол = 'Ж' THEN b.кол_пациентов END), 0)
            THEN 'чаще у женщин'
            ELSE 'одинаково'
        END AS вывод,

        r.всего_пациентов

    FROM ranked r
    LEFT JOIN base b USING (класс_заболевания)
    GROUP BY 1, r.всего_пациентов
)

SELECT *
FROM final
ORDER BY всего_пациентов DESC;
