SELECT
    link,
    team,
    TRIM(type)                                                              AS tags,
    title,
    raised_by,
    TO_TIMESTAMP(TRIM(date_created), 'YYYY-MM-DD HH24:MI:SS')
        AT TIME ZONE 'UTC'                                                  AS date_created_at,
    CASE
        WHEN resolution_date IS NOT NULL AND TRIM(resolution_date) != ''
        THEN TO_TIMESTAMP(TRIM(resolution_date), 'YYYY-MM-DD HH24:MI:SS')
            AT TIME ZONE 'UTC'
        ELSE NULL
    END                                                                     AS resolved_at,
    CASE
        WHEN resolution_date IS NOT NULL AND TRIM(resolution_date) != ''
        THEN TRUE
        ELSE FALSE
    END                                                                     AS is_resolved,
    CASE
        WHEN resolution_date IS NOT NULL AND TRIM(resolution_date) != ''
        THEN ROUND(
            EXTRACT(EPOCH FROM (
                TO_TIMESTAMP(TRIM(resolution_date), 'YYYY-MM-DD HH24:MI:SS')
                - TO_TIMESTAMP(TRIM(date_created), 'YYYY-MM-DD HH24:MI:SS')
            )) / 3600.0
        , 2)
        ELSE NULL
    END                                                                     AS resolution_time_hours
FROM {{ source('support', 'support_tickets') }}
WHERE date_created IS NOT NULL AND TRIM(date_created) != ''
    AND TRIM(team) ILIKE '%Engineering%'
