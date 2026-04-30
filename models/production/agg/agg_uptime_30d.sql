WITH beats_with_duration AS (
    SELECT
        monitor_id,
        monitor_name,
        monitor_group,
        monitor_url,
        monitor_type,
        is_up,
        check_interval_seconds,
        EXTRACT(EPOCH FROM (
            LEAD(beat_time) OVER (PARTITION BY monitor_id ORDER BY beat_time)
            - beat_time
        )) / 60.0                                                           AS minutes_in_state
    FROM {{ ref('monitor_beats') }}
    WHERE beat_time >= CURRENT_DATE - INTERVAL '30 days'
)

SELECT
    monitor_group,
    ROUND(SUM(
        CASE WHEN is_up = TRUE THEN COALESCE(minutes_in_state, check_interval_seconds / 60.0) ELSE 0 END
    ), 2)                                                                   AS up_minutes,
    ROUND(SUM(
        CASE WHEN is_up = FALSE THEN COALESCE(minutes_in_state, check_interval_seconds / 60.0) ELSE 0 END
    ), 2)                                                                   AS down_minutes,
    ROUND(SUM(COALESCE(minutes_in_state, check_interval_seconds / 60.0)), 2)
                                                                            AS total_minutes,
    ROUND(100.0 * SUM(
        CASE WHEN is_up = TRUE THEN COALESCE(minutes_in_state, check_interval_seconds / 60.0) ELSE 0 END
    ) / NULLIF(SUM(COALESCE(minutes_in_state, check_interval_seconds / 60.0)), 0), 2)
                                                                            AS uptime_pct
FROM beats_with_duration
GROUP BY monitor_group
