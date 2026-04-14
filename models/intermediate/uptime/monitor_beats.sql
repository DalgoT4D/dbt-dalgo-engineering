{{
    config(
        materialized='incremental',
        unique_key='beat_id',
        incremental_strategy='delete+insert'
    )
}}

WITH beats AS (
    SELECT
        id::int                                         AS beat_id,
        monitor_id::int                                 AS monitor_id,
        status                                          AS is_up,
        ping::int                                       AS ping_ms,
        msg                                             AS message,
        duration::int                                   AS duration_ms,
        retries::int                                    AS retries,
        down_count::int                                 AS down_count,
        important                                       AS is_important,
        TO_TIMESTAMP(TRIM(time), 'YYYY-MM-DD HH24:MI:SS')
            AT TIME ZONE 'UTC'                          AS beat_time,
        CASE
            WHEN end_time IS NOT NULL AND TRIM(end_time) != ''
            THEN TO_TIMESTAMP(TRIM(end_time), 'YYYY-MM-DD HH24:MI:SS')
                AT TIME ZONE 'UTC'
            ELSE NULL
        END                                             AS beat_end_time,
        _airbyte_extracted_at
    FROM {{ source('uptime', 'beats') }}
    {% if is_incremental() %}
    WHERE _airbyte_extracted_at > (SELECT MAX(_airbyte_extracted_at) FROM {{ this }})
    {% endif %}
),

child_monitors AS (
    SELECT
        id::int                                         AS monitor_id,
        name                                            AS monitor_name,
        url                                             AS monitor_url,
        type                                            AS monitor_type,
        active                                          AS is_active,
        parent::int                                     AS parent_id,
        interval::int                                   AS check_interval_seconds,
        timeout::int                                    AS timeout_seconds,
        maxretries::int                                 AS max_retries
    FROM {{ source('uptime', 'monitors') }}
    WHERE parent IS NOT NULL
),

parent_monitors AS (
    SELECT
        id::int                                         AS monitor_id,
        name                                            AS monitor_group
    FROM {{ source('uptime', 'monitors') }}
    WHERE parent IS NULL
),

monitors AS (
    SELECT
        c.*,
        p.monitor_group
    FROM child_monitors c
    INNER JOIN parent_monitors p
        ON c.parent_id = p.monitor_id
)

SELECT
    b.beat_id,
    b.monitor_id,
    m.monitor_name,
    m.monitor_group,
    m.monitor_url,
    m.monitor_type,
    m.is_active,
    m.check_interval_seconds,
    m.timeout_seconds,
    m.max_retries,
    b.is_up,
    b.ping_ms,
    b.message,
    b.duration_ms,
    b.retries,
    b.down_count,
    b.is_important,
    b.beat_time,
    b.beat_end_time,
    b._airbyte_extracted_at
FROM beats b
INNER JOIN monitors m
    ON b.monitor_id = m.monitor_id
