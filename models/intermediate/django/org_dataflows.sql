SELECT 
    df.id,
    df.name,
    df.deployment_id,
    df.deployment_name,
    df.cron,
    df.dataflow_type,
    df.created_at,
    df.updated_at,
    df.org_id,
    o.org_name,
    o.org_slug,
    o.base_plan,
    o.features,
    o.subscription_duration
FROM {{ source('django', 'dataflows') }} df
LEFT JOIN {{ ref('all_orgs') }} o 
    ON df.org_id = o.org_id