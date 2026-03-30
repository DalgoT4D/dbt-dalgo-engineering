SELECT
    o.org_id,
    o.org_name,
    o.org_slug,
    o.base_plan,
    c.chart_type,
    COUNT(*) AS total_charts
FROM {{ source('django', 'ddpui_chart') }} c
INNER JOIN {{ ref('all_orgs') }} o ON c.org_id = o.org_id
GROUP BY
    o.org_id,
    o.org_name,
    o.org_slug,
    o.base_plan,
    c.chart_type
