WITH dimension_spine AS (
    SELECT 
        c.date_day,
        c.granularity,
        c.period_start,
        c.period_end,
        c.year,
        c.month
    FROM {{ ref('dim_calendar') }} c
),

airbyte_jobs_agg AS (
    SELECT 
        ds.date_day,
        ds.granularity,
        ds.period_start,
        ds.period_end,
        ds.year,
        ds.month,
        COUNT(CASE WHEN ja.classified_status = 'success' THEN 1 END) as total_successes,
        COUNT(CASE WHEN ja.classified_status = 'failure' THEN 1 END) as total_failures,
        COUNT(CASE WHEN ja.classified_status = 'silent_failure' THEN 1 END) as total_silent_failures
    FROM dimension_spine ds
    LEFT JOIN {{ ref('job_attempts') }} ja 
        ON DATE(ja.job_created_at) BETWEEN ds.period_start AND ds.period_end
    GROUP BY 
        ds.date_day, ds.granularity, ds.period_start, ds.period_end, ds.year, ds.month
)

SELECT * FROM airbyte_jobs_agg
ORDER BY date_day