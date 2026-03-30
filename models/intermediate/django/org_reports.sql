WITH snapshot_stats AS (
    SELECT
        org_id,
        COUNT(*) AS total_snapshots
    FROM {{ source('django', 'report_snapshot') }}
    GROUP BY org_id
),

comment_stats AS (
    SELECT
        org_id,
        COUNT(*) AS total_comments,
        COUNT(CASE WHEN is_deleted = FALSE THEN 1 END) AS active_comments,
        COUNT(DISTINCT author_id) AS unique_commenting_users
    FROM {{ source('django', 'comment') }}
    GROUP BY org_id
)

SELECT
    o.org_id,
    o.org_name,
    o.org_slug,
    o.base_plan,
    COALESCE(ss.total_snapshots, 0) AS total_snapshots,
    COALESCE(cs.total_comments, 0) AS total_comments,
    COALESCE(cs.active_comments, 0) AS active_comments,
    COALESCE(cs.unique_commenting_users, 0) AS unique_commenting_users
FROM {{ ref('all_orgs') }} o
LEFT JOIN snapshot_stats ss
    ON o.org_id = ss.org_id
LEFT JOIN comment_stats cs
    ON o.org_id = cs.org_id
