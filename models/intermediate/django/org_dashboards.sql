WITH dashboard_stats AS (
    SELECT
        org_id,
        COUNT(*) AS total_dashboards,
        COUNT(CASE WHEN is_public = TRUE THEN 1 END) AS public_dashboards
    FROM {{ source('django', 'dashboard') }}
    GROUP BY org_id
),

chart_stats AS (
    SELECT
        org_id,
        COUNT(*) AS total_charts
    FROM {{ source('django', 'ddpui_chart') }}
    GROUP BY org_id
)

SELECT
    o.org_id,
    o.org_name,
    o.org_slug,
    o.base_plan,
    COALESCE(ds.total_dashboards, 0) AS total_dashboards,
    COALESCE(ds.public_dashboards, 0) AS public_dashboards,
    COALESCE(cs.total_charts, 0) AS total_charts
FROM {{ ref('all_orgs') }} o
LEFT JOIN dashboard_stats ds ON o.org_id = ds.org_id
LEFT JOIN chart_stats cs ON o.org_id = cs.org_id
ORDER BY total_dashboards DESC, total_charts DESC
