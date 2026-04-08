WITH dimension_spine AS (
    SELECT
        date_day,
        granularity,
        period_start,
        period_end,
        year,
        month,
        month_name
    FROM {{ ref('dim_calendar') }}
    WHERE date_day >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months')
)

SELECT
    ds.date_day,
    ds.granularity,
    ds.period_start,
    ds.period_end,
    ds.year,
    ds.month,
    ds.month_name,
    COUNT(st.link)                                                          AS tickets_opened,
    SUM(CASE WHEN st.is_resolved = TRUE THEN 1 ELSE 0 END)                 AS tickets_resolved,
    ROUND(AVG(CASE WHEN st.is_resolved = TRUE
              THEN st.resolution_time_hours END), 2)                        AS avg_resolution_time_hours,
    COUNT(CASE WHEN st.tags ILIKE '%platform-issue%' THEN 1 END)           AS platform_issue_tickets,
    COUNT(CASE WHEN st.tags ILIKE '%knowledge-gap%' THEN 1 END)            AS knowledge_gap_tickets,
    COUNT(CASE WHEN st.tags ILIKE '%bug%' THEN 1 END)                      AS bug_tickets
FROM dimension_spine ds
LEFT JOIN {{ ref('support_tickets') }} st
    ON st.date_created_at BETWEEN ds.period_start AND ds.period_end
GROUP BY
    ds.date_day, ds.granularity, ds.period_start, ds.period_end,
    ds.year, ds.month, ds.month_name
ORDER BY ds.date_day
