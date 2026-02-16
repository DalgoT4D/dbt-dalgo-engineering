WITH user_counts AS (
    SELECT 
        org_id,
        COUNT(*) as total_users,
        COUNT(CASE WHEN role_slug = 'super-admin' THEN 1 END) as super_admin_users,
        COUNT(CASE WHEN role_slug = 'account-manager' THEN 1 END) as account_manager_users,
        COUNT(CASE WHEN role_slug = 'pipeline-manager' THEN 1 END) as pipeline_manager_users,
        COUNT(CASE WHEN role_slug = 'analyst' THEN 1 END) as analyst_users,
        COUNT(CASE WHEN role_slug = 'guest' THEN 1 END) as guest_users
    FROM {{ ref('org_users') }}
    GROUP BY org_id
)

SELECT 
    o.id as org_id,
    o.name as org_name,
    o.slug as org_slug,
    o.created_at as org_created_at,
    o.updated_at as org_updated_at,
    o.queue_config as queue_config,
    o.viz_url as superset_viz_url,
    o.viz_login_type as superset_viz_login_type,
    p.base_plan as base_plan,
    p.features as features,
    p.subscription_duration as subscription_duration,
    p.superset_included as superset_included,
    COALESCE(uc.total_users, 0) as total_users,
    COALESCE(uc.super_admin_users, 0) as super_admin_users,
    COALESCE(uc.account_manager_users, 0) as account_manager_users,
    COALESCE(uc.pipeline_manager_users, 0) as pipeline_manager_users,
    COALESCE(uc.analyst_users, 0) as analyst_users,
    COALESCE(uc.guest_users, 0) as guest_users
FROM {{ source('django', 'orgs') }} o
LEFT JOIN {{ source('django', 'orgplans') }} p 
    ON o.id = p.org_id
LEFT JOIN user_counts uc
    ON o.id = uc.org_id