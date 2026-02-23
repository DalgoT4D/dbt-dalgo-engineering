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
    CASE 
        WHEN tr.flow_run_execution_chain IS NOT NULL THEN
            SUBSTRING(
                SPLIT_PART(tr.flow_run_execution_chain, ' -> ', -1)
                FROM '\(([^)]+)\)'
            )
        ELSE NULL
    END as last_step_state_name
FROM {{ ref('deployments') }} d 
INNER JOIN flow_runs_with_queue_info frq
    ON d.deployment_id = frq.deployment_id
INNER JOIN {{ ref('task_runs') }} tr
    ON frq.flow_run_id = tr.flow_run_id