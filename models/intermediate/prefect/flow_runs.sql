-- look flow runs from last 4 months

with flow_runs_with_queue_info as (
    SELECT 
        fr.id as flow_run_id,
        fr.name as flow_run_name,
        fr.deployment_id,
        fr.state_type,
        fr.state_name,
        fr.start_time,
        fr.expected_start_time,
        fr.end_time,
        fr.total_run_time,
        fr.auto_scheduled,
        wq.work_queue_id
    FROM {{ source('prefect', 'flow_run') }} fr
    LEFT JOIN {{ ref('work_queues') }} wq
        ON fr.work_queue_id = wq.work_queue_id AND fr.created >= CURRENT_DATE - INTERVAL '4 months' 
)

SELECT 
    d.deployment_name,
    d.org_slug,
    d.deployment_type,
    d.deployment_params_chain,
    frq.*,
    tr.flow_run_execution_chain,
    tr.last_step_state_name,
    tr.last_step,
    CASE 
        WHEN tr.last_step LIKE '%connection-sync%' 
             OR tr.last_step LIKE '%connection-flow%' THEN 'airbyte-sync'
        WHEN tr.last_step LIKE '%clear-connection%' THEN 'clear-connection'
        WHEN tr.last_step LIKE '%dbt-run%' THEN 'dbt-run'
        WHEN tr.last_step LIKE '%dbt-test%' THEN 'dbt-test'
        WHEN tr.last_step LIKE '%dbt-deps%' THEN 'dbt-deps'
        WHEN tr.last_step LIKE '%git-pull%' THEN 'git-pull'
        WHEN tr.last_step LIKE '%refresh-schema%' THEN 'airbyte-schema'
        ELSE tr.last_step
    END as last_step_cleaned,
    CASE 
        WHEN tr.last_step LIKE '%connection-sync%' 
             OR tr.last_step LIKE '%connection-flow%'
             OR tr.last_step LIKE '%refresh-schema%'
             OR tr.last_step LIKE '%clear-connection%' THEN 'sync'
        WHEN tr.last_step LIKE '%dbt-run%' 
             OR tr.last_step LIKE '%dbt-test%'
             OR tr.last_step LIKE '%dbt-deps%'
             OR tr.last_step LIKE '%git-pull%' THEN 'transform'
        ELSE 'other'
    END as last_step_category
FROM {{ ref('deployments') }} d 
INNER JOIN flow_runs_with_queue_info frq
    ON d.deployment_id = frq.deployment_id
INNER JOIN {{ ref('task_runs') }} tr
    ON frq.flow_run_id = tr.flow_run_id