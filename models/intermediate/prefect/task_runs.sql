-- look at task runs from last 5 months

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
    WHERE created >= CURRENT_DATE - INTERVAL '5 months'
),

task_runs_with_last_step AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY flow_run_id ORDER BY created DESC) as rn
    FROM task_runs_filtered
)

SELECT
    flow_run_id,
    string_agg(
        CONCAT(task_name, '(', state_name, ')'), 
        ' -> ' 
        ORDER BY created
    ) as flow_run_execution_chain,
    MAX(CASE WHEN rn = 1 THEN state_name END) as last_step_state_name,
    MAX(CASE WHEN rn = 1 THEN task_name END) as last_step
FROM task_runs_with_last_step
GROUP BY flow_run_id