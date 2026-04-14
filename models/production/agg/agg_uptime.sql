{{
    config(
        materialized='incremental',
        unique_key=['monitor_id', 'beat_month'],
        incremental_strategy='delete+insert'
    )
}}

WITH beats_with_duration AS (
    SELECT
        monitor_id,
        monitor_name,
        monitor_group,
        monitor_url,
        monitor_type,
        is_up,
        beat_time,
        check_interval_seconds,
        DATE_TRUNC('month', beat_time)::date                                AS beat_month,
        EXTRACT(EPOCH FROM (
            LEAD(beat_time) OVER (PARTITION BY monitor_id ORDER BY beat_time)
            - beat_time
        )) / 60.0                                                           AS minutes_in_state
    FROM {{ ref('monitor_beats') }}
    {% if is_incremental() %}
    WHERE DATE_TRUNC('month', beat_time) >= DATE_TRUNC('month', CURRENT_DATE)
    {% endif %}
),

monthly_uptime AS (
    SELECT
        beat_month,
        monitor_id,
        monitor_name,
        monitor_group,
        monitor_url,
        monitor_type,
        ROUND(SUM(
            CASE WHEN is_up = TRUE THEN COALESCE(minutes_in_state, check_interval_seconds / 60.0) ELSE 0 END
        ), 2)                                                               AS up_minutes,
        ROUND(SUM(
            CASE WHEN is_up = FALSE THEN COALESCE(minutes_in_state, check_interval_seconds / 60.0) ELSE 0 END
        ), 2)                                                               AS down_minutes
    FROM beats_with_duration
    GROUP BY beat_month, monitor_id, monitor_name, monitor_group, monitor_url, monitor_type
)

SELECT
    beat_month,
    monitor_id,
    monitor_name,
    monitor_group,
    monitor_url,
    monitor_type,
    up_minutes,
    down_minutes,
    up_minutes + down_minutes                                               AS total_minutes,
    ROUND(100.0 * up_minutes / NULLIF(up_minutes + down_minutes, 0), 2)    AS uptime_pct
FROM monthly_uptime
