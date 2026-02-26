SELECT 
    org_name,
    dataflow_deployment_name as deployment_name,
    deployment_id,
    state_name as status,
    start_time
FROM {{ ref('org_pipeline_runs') }}
WHERE flow_run_id IS NOT NULL