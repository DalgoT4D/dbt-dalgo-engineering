WITH task_runs_filtered AS (
    SELECT 
        id as task_run_id,
        name as task_name,
        state_type,
        state_name,
        start_time,
        end_time,
        total_run_time,
        task_key,
        flow_run_id,
        created
    FROM {{ source('prefect', 'task_run') }}
    WHERE created >= CURRENT_DATE - INTERVAL '4 months'
)

SELECT
    flow_run_id,
    string_agg(
        CONCAT(task_name, '(', state_name, ')'), 
        ' -> ' 
        ORDER BY created
    ) as flow_run_execution_chain
FROM task_runs_filtered
GROUP BY flow_run_id