WITH date_spine_raw AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="DATE_TRUNC('month', (SELECT MIN(created_at) FROM " ~ source('django', 'report_snapshot') ~ "))::date",
        end_date="current_date"
    ) }}
),

monthly_spine AS (
    SELECT DISTINCT
        DATE_TRUNC('month', date_day)::date AS date_day,
        'monthly' AS granularity,
        (DATE_TRUNC('month', date_day)::timestamp AT TIME ZONE 'UTC')::timestamptz AS period_start,
        ((DATE_TRUNC('month', date_day) + INTERVAL '1 month - 1 second')::timestamp AT TIME ZONE 'UTC')::timestamptz AS period_end,
        EXTRACT(year FROM DATE_TRUNC('month', date_day)) AS year,
        EXTRACT(month FROM DATE_TRUNC('month', date_day)) AS month,
        NULL::integer AS day,
        NULL::integer AS day_of_week,
        NULL::text AS day_name,
        TO_CHAR(DATE_TRUNC('month', date_day), 'Month') AS month_name,
        TRIM(TO_CHAR(DATE_TRUNC('month', date_day), 'Month')) || ' ' || EXTRACT(year FROM DATE_TRUNC('month', date_day))::text AS month_year
    FROM date_spine_raw
),

comments_with_users AS (
    SELECT
        c.org_id,
        c.created_at,
        c.is_deleted,
        c.author_id
    FROM {{ source('django', 'comment') }} c
    INNER JOIN {{ ref('org_users') }} ou
        ON c.author_id = ou.orguser_id
),

dimension_spine AS (
    SELECT
        ms.date_day,
        ms.granularity,
        ms.period_start,
        ms.period_end,
        ms.year,
        ms.month,
        ms.day,
        ms.day_of_week,
        ms.day_name,
        ms.month_name,
        ms.month_year,
        org_dim.org_id,
        org_dim.org_name,
        org_dim.org_slug,
        org_dim.base_plan
    FROM monthly_spine ms
    CROSS JOIN (
        SELECT DISTINCT org_id, org_name, org_slug, base_plan
        FROM {{ ref('all_orgs') }}
    ) org_dim
)

SELECT
    ds.date_day,
    ds.granularity,
    ds.period_start,
    ds.period_end,
    ds.year,
    ds.month,
    ds.day,
    ds.day_of_week,
    ds.day_name,
    ds.month_name,
    ds.month_year,
    ds.org_id,
    ds.org_name,
    ds.org_slug,
    ds.base_plan,
    COUNT(rs.id) AS total_snapshots,
    COUNT(CASE WHEN rs.summary IS NOT NULL AND rs.summary != '' THEN 1 END) AS total_executive_summaries,
    COUNT(c.author_id) AS total_comments,
    COUNT(CASE WHEN c.is_deleted = FALSE THEN 1 END) AS active_comments,
    COUNT(DISTINCT c.author_id) AS unique_commenting_users
FROM dimension_spine ds
LEFT JOIN {{ source('django', 'report_snapshot') }} rs
    ON ds.org_id = rs.org_id
    AND rs.created_at BETWEEN ds.period_start AND ds.period_end
LEFT JOIN comments_with_users c
    ON ds.org_id = c.org_id
    AND c.created_at BETWEEN ds.period_start AND ds.period_end
GROUP BY
    ds.date_day, ds.granularity, ds.period_start, ds.period_end,
    ds.year, ds.month, ds.day, ds.day_of_week, ds.day_name, ds.month_name, ds.month_year,
    ds.org_id, ds.org_name, ds.org_slug, ds.base_plan
ORDER BY ds.date_day DESC, ds.org_slug
