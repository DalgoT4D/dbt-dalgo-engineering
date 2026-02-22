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
        org_dim.org_slug,
        wq.work_queue_id
    FROM {{ ref('dim_calendar') }} c
    CROSS JOIN (
        SELECT DISTINCT org_id, org_name, org_slug
        FROM {{ ref('org_pipeline_runs') }}
        WHERE org_id IS NOT NULL
    ) org_dim
    CROSS JOIN (
        SELECT work_queue_id
        FROM {{ ref('work_queues') }}
    ) wq
),

pipeline_runs_agg AS (
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
        ds.work_queue_id,
        COUNT(opr.flow_run_id) as total_pipeline_runs,
        COUNT(CASE WHEN opr.state_type = 'COMPLETED' THEN 1 END) as total_successful_runs,
        COUNT(CASE WHEN opr.state_type IN ('FAILED', 'CRASHED') THEN 1 END) as total_failed_runs,
        COUNT(CASE WHEN opr.state_type NOT IN ('COMPLETED', 'FAILED', 'CRASHED') THEN 1 END) as total_other_runs,
        COUNT(CASE WHEN opr.auto_scheduled = TRUE THEN 1 END) as total_scheduled_runs,
        COUNT(CASE WHEN opr.auto_scheduled = FALSE THEN 1 END) as total_manual_runs
    FROM dimension_spine ds
    LEFT JOIN {{ ref('org_pipeline_runs') }} opr 
        ON ds.org_id = opr.org_id 
        AND ds.work_queue_id = opr.work_queue_id
        AND opr.expected_start_time BETWEEN ds.period_start AND ds.period_end
    GROUP BY 
        ds.date_day, ds.granularity, ds.period_start, ds.period_end, ds.year, ds.month,
        ds.org_id, ds.org_name, ds.org_slug,
        ds.work_queue_id
)

SELECT pipeline_runs_agg.*, wq.work_pool_name, wq.work_pool_type, wq.work_queue_name FROM pipeline_runs_agg
LEFT JOIN {{ ref('work_queues') }} wq
    ON pipeline_runs_agg.work_queue_id = wq.work_queue_id
ORDER BY date_day, org_slug, pipeline_runs_agg.work_queue_id