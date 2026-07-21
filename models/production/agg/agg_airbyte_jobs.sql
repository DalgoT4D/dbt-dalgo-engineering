WITH dimension_spine AS (
    SELECT
        c.date_day,
        c.granularity,
        c.period_start,
        c.period_end,
        c.year,
        c.month,
        org_dim.org_id,
        org_dim.org_name,
        org_dim.org_slug
    FROM {{ ref('dim_calendar') }} c
    CROSS JOIN (
        SELECT DISTINCT org_id, org_name, org_slug
        FROM {{ ref('all_orgs') }}
        WHERE airbyte_workspace_id IS NOT NULL
    ) org_dim
    WHERE c.date_day >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months')
),

airbyte_jobs_agg AS (
    SELECT
        ds.date_day,
        ds.granularity,
        ds.period_start,
        ds.period_end,
        ds.year,
        ds.month,
        ds.org_id,
        ds.org_name,
        ds.org_slug,
        COUNT(CASE WHEN ja.classified_status = 'success' THEN 1 END) as total_successes,
        COUNT(CASE WHEN ja.classified_status = 'failure' THEN 1 END) as total_failures,
        COUNT(CASE WHEN ja.classified_status = 'silent_failure' THEN 1 END) as total_silent_failures,
        COUNT(CASE WHEN ja.classified_status = 'running' THEN 1 END) as total_running,
        SUM(ja.total_records_emitted) as total_records_emitted,
        ROUND(SUM(ja.total_bytes_emitted) / 1073741824.0, 2) as total_gb_emitted,
        SUM(ja.total_records_committed) as total_records_committed,
        ROUND(SUM(ja.total_bytes_committed) / 1073741824.0, 2) as total_gb_committed,
        SUM(ja.total_records_rejected) as total_records_rejected,
        ROUND(SUM(ja.sync_duration_minutes)::numeric, 2) as total_sync_duration_minutes,
        ROUND(AVG(ja.sync_duration_minutes)::numeric, 2) as avg_sync_duration_minutes,
        ROUND(MAX(ja.sync_duration_minutes)::numeric, 2) as max_sync_duration_minutes
    FROM dimension_spine ds
    LEFT JOIN {{ ref('job_attempts') }} ja
        ON ds.org_id = ja.org_id
        AND DATE(ja.job_created_at) BETWEEN ds.period_start AND ds.period_end
    GROUP BY
        ds.date_day, ds.granularity, ds.period_start, ds.period_end, ds.year, ds.month,
        ds.org_id, ds.org_name, ds.org_slug
)

SELECT * FROM airbyte_jobs_agg
ORDER BY date_day, org_slug