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
        ON fr.work_queue_id = wq.work_queue_id
)

SELECT 
    d.deployment_name,
    d.org_slug,
    frq.*
FROM {{ ref('deployments') }} d 
INNER JOIN flow_runs_with_queue_info frq
    ON d.deployment_id = frq.deployment_id