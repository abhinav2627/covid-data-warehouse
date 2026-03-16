WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_COVID_DATA') }}
)

SELECT
    ISO_CODE                                    AS iso_code,
    CONTINENT                                   AS continent,
    LOCATION                                    AS country,
    DATE                                        AS report_date,
    NULLIF(TOTAL_CASES, 0)                      AS total_cases,
    NULLIF(NEW_CASES, 0)                        AS new_cases,
    NULLIF(TOTAL_DEATHS, 0)                     AS total_deaths,
    NULLIF(NEW_DEATHS, 0)                       AS new_deaths,
    NULLIF(TOTAL_VACCINATIONS, 0)               AS total_vaccinations,
    NULLIF(PEOPLE_VACCINATED, 0)                AS people_vaccinated,
    NULLIF(PEOPLE_FULLY_VACCINATED, 0)          AS people_fully_vaccinated,
    NULLIF(POPULATION, 0)                       AS population,
    NULLIF(MEDIAN_AGE, 0)                       AS median_age,
    NULLIF(GDP_PER_CAPITA, 0)                   AS gdp_per_capita,
    CASE
        WHEN TOTAL_CASES > 0 AND POPULATION > 0
        THEN ROUND((TOTAL_DEATHS / TOTAL_CASES) * 100, 4)
        ELSE NULL
    END                                         AS case_fatality_rate,
    CASE
        WHEN PEOPLE_FULLY_VACCINATED > 0 AND POPULATION > 0
        THEN ROUND((PEOPLE_FULLY_VACCINATED / POPULATION) * 100, 2)
        ELSE NULL
    END                                         AS vaccination_rate_pct
FROM source
WHERE CONTINENT IS NOT NULL
  AND DATE IS NOT NULL
