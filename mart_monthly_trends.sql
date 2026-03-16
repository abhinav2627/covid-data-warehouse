WITH stg AS (
    SELECT * FROM {{ ref('stg_covid') }}
)

SELECT
    continent,
    DATE_TRUNC('month', report_date)    AS month,
    COUNT(DISTINCT country)             AS countries_reporting,
    ROUND(SUM(new_cases), 0)            AS monthly_new_cases,
    ROUND(SUM(new_deaths), 0)           AS monthly_new_deaths,
    ROUND(AVG(vaccination_rate_pct), 2) AS avg_vaccination_rate,
    ROUND(AVG(case_fatality_rate), 4)   AS avg_case_fatality_rate
FROM stg
WHERE new_cases IS NOT NULL
GROUP BY continent, DATE_TRUNC('month', report_date)
ORDER BY month, continent
