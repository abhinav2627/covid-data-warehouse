WITH kpis AS (
    SELECT * FROM {{ ref('mart_country_kpis') }}
)

SELECT
    rank_by_vaccination                 AS vaccination_rank,
    country,
    continent,
    population,
    ROUND(vaccination_rate_pct, 2)      AS vaccination_rate_pct,
    ROUND(people_fully_vaccinated, 0)   AS people_fully_vaccinated,
    ROUND(case_fatality_rate, 4)        AS case_fatality_rate,
    ROUND(gdp_per_capita, 0)            AS gdp_per_capita
FROM kpis
WHERE vaccination_rate_pct IS NOT NULL
ORDER BY vaccination_rank
LIMIT 50
