WITH stg AS (
    SELECT * FROM {{ ref('stg_covid') }}
),

latest AS (
    SELECT
        iso_code,
        continent,
        country,
        population,
        median_age,
        gdp_per_capita,
        MAX(report_date)                AS latest_date,
        MAX(total_cases)                AS total_cases,
        MAX(total_deaths)               AS total_deaths,
        MAX(people_fully_vaccinated)    AS people_fully_vaccinated,
        MAX(vaccination_rate_pct)       AS vaccination_rate_pct,
        MAX(case_fatality_rate)         AS case_fatality_rate,
        SUM(new_cases)                  AS sum_new_cases,
        SUM(new_deaths)                 AS sum_new_deaths,
        AVG(new_cases)                  AS avg_daily_cases,
        AVG(new_deaths)                 AS avg_daily_deaths
    FROM stg
    GROUP BY iso_code, continent, country, population, median_age, gdp_per_capita
)

SELECT
    iso_code,
    continent,
    country,
    population,
    median_age,
    gdp_per_capita,
    latest_date,
    ROUND(total_cases, 0)               AS total_cases,
    ROUND(total_deaths, 0)              AS total_deaths,
    ROUND(people_fully_vaccinated, 0)   AS people_fully_vaccinated,
    ROUND(vaccination_rate_pct, 2)      AS vaccination_rate_pct,
    ROUND(case_fatality_rate, 4)        AS case_fatality_rate,
    ROUND(avg_daily_cases, 1)           AS avg_daily_cases,
    ROUND(avg_daily_deaths, 2)          AS avg_daily_deaths,
    RANK() OVER (ORDER BY total_cases DESC NULLS LAST)          AS rank_by_cases,
    RANK() OVER (ORDER BY total_deaths DESC NULLS LAST)         AS rank_by_deaths,
    RANK() OVER (ORDER BY vaccination_rate_pct DESC NULLS LAST) AS rank_by_vaccination
FROM latest
