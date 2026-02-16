SELECT 
    o.id as org_id,
    o.name as org_name,
    o.slug as org_slug,
    o.created_at as org_created_at,
    o.updated_at as org_updated_at,
    o.queue_config as queue_config,
    p.base_plan as base_plan,
    p.features as features,
    p.subscription_duration as subscription_duration,
    p.superset_included as superset_included
FROM {{ source('django', 'orgs') }} o
LEFT JOIN {{ source('django', 'orgplans') }} p 
    ON o.id = p.org_id