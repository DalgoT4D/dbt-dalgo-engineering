SELECT 
    id as deployment_id,
    name as deployment_name,
    tags,
    tags::jsonb->>0 as org_slug,
    created as created_at,
    updated as updated_at
FROM {{ source('prefect', 'deployment') }}
WHERE tags IS NOT NULL AND jsonb_array_length(tags::jsonb) > 0