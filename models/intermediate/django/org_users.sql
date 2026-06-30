SELECT
    ou.id as orguser_id,
    ou.org_id,
    au.email as user_email,
    r.slug as role_slug,
    r.name as role_name,
    r.level as role_level,
    o.name as org_name,
    o.slug as org_slug,
    o.created_at as org_created_at,
    o.updated_at as org_updated_at,
    o.airbyte_workspace_id,
    o.viz_url as superset_viz_url,
    o.viz_login_type as superset_viz_login_type,
    p.base_plan as base_plan,
    p.features as features,
    p.subscription_duration as subscription_duration,
    p.superset_included as superset_included
FROM {{ source('django', 'orgusers') }} ou
LEFT JOIN {{ source('django', 'roles') }} r
    ON ou.new_role_id = r.id
LEFT JOIN {{ source('django', 'users') }} au
    ON ou.user_id = au.id
LEFT JOIN {{ source('django', 'orgs') }} o
    ON ou.org_id = o.id
LEFT JOIN {{ source('django', 'orgplans') }} p
    ON o.id = p.org_id
WHERE ou.org_id IS NOT NULL AND au.email IS NOT NULL AND au.email NOT LIKE '%@projecttech4dev.org' -- remove internal team members