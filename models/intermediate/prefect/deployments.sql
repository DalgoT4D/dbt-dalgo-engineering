-- look deployments from last 4 months

WITH deployment_tasks AS (
    SELECT 
        id as deployment_id,
        name as deployment_name,
        tags,
        tags::jsonb->>0 as org_slug,
        created as created_at,
        updated as updated_at,
        (
            SELECT string_agg(
                task_item->>'slug', 
                ' -> ' 
                ORDER BY (task_item->>'seq')::int
            )
            FROM jsonb_array_elements(parameters::jsonb->'config'->'tasks') AS task_item
        ) AS deployment_params_chain,
        (
            SELECT bool_or(task_item->>'slug' LIKE 'airbyte-%')
            FROM jsonb_array_elements(parameters::jsonb->'config'->'tasks') AS task_item
        ) AS has_airbyte_task,
        (
            SELECT bool_or(task_item->>'slug' LIKE 'dbt-%' OR task_item->>'slug' = 'git-pull')
            FROM jsonb_array_elements(parameters::jsonb->'config'->'tasks') AS task_item
        ) AS has_dbt_task,
        (
            SELECT bool_or(task_item->>'slug' = 'generate-edr')
            FROM jsonb_array_elements(parameters::jsonb->'config'->'tasks') AS task_item
        ) AS has_edr_task
    FROM {{ source('prefect', 'deployment') }}
    WHERE 
        tags IS NOT NULL AND 
        jsonb_array_length(tags::jsonb) > 0 AND 
        created >= CURRENT_DATE - INTERVAL '4 months'
)

SELECT 
    deployment_id,
    deployment_name,
    tags,
    org_slug,
    created_at,
    updated_at,
    deployment_params_chain,
    CASE 
        WHEN has_airbyte_task AND has_dbt_task THEN 'airbyte+dbt'
        WHEN has_dbt_task AND NOT has_airbyte_task THEN 'dbt'
        WHEN has_airbyte_task AND NOT has_dbt_task THEN 'airbyte'
        WHEN has_edr_task THEN 'edr'
        ELSE 'other'
    END AS deployment_type
FROM deployment_tasks 