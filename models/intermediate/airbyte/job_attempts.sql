WITH latest_attempts AS (
    SELECT
        id as attempt_id,
        job_id,
        MAX(attempt_number) as max_attempt_number
    FROM {{ source('airbyte', 'attempts') }}
    WHERE created_at >= CURRENT_DATE - INTERVAL '2 months'
    GROUP BY id, job_id
),
attempts_normalized AS (
    SELECT
        a.id as attempt_id,
        a.job_id,
        a.attempt_number,
        a.status as attempt_status,
        a.created_at as attempt_created_at,
        a.updated_at as attempt_updated_at,
        a.ended_at as attempt_ended_at,
        a.output,
        a.output::jsonb -> 'sync' -> 'failures' -> 0 ->> 'failureType' as failure_type,
        a.output::jsonb -> 'sync' -> 'failures' -> 0 ->> 'failureOrigin' as failure_origin,
        a.output::jsonb -> 'sync' -> 'failures' -> 0 ->> 'externalMessage' as external_message,
        a.output::jsonb -> 'sync' -> 'failures' -> 0 ->> 'internalMessage' as internal_message,
        jsonb_array_length(a.output::jsonb -> 'sync' -> 'failures') as failure_count
    FROM {{ source('airbyte', 'attempts') }} a
    INNER JOIN latest_attempts la
        ON a.id = la.attempt_id
        AND a.attempt_number = la.max_attempt_number
)
SELECT 
    an.attempt_id,
    an.attempt_number,
    an.attempt_status,
    an.attempt_created_at,
    an.attempt_updated_at,
    an.attempt_ended_at,
    an.output,
    an.failure_type,
    an.failure_origin,
    an.external_message,
    an.internal_message,
    an.failure_count,
    jobs.id AS job_id,
    jobs.scope AS connection_id,
    jobs.config_type,
    jobs.status AS job_status,
    jobs.created_at AS job_created_at,
    jobs.updated_at AS job_updated_at,
    jobs.config::jsonb -> 'sync' ->> 'workspaceId' AS workspace_id,
    CASE 
        WHEN jobs.status = 'succeeded' 
            AND an.attempt_status = 'succeeded' 
            AND (an.failure_type IS NOT NULL 
                 OR an.failure_origin IS NOT NULL 
                 OR an.external_message IS NOT NULL 
                 OR an.internal_message IS NOT NULL)
        THEN 'silent_failure'
        WHEN jobs.status = 'failed' THEN 'failure'
        WHEN jobs.status = 'cancelled' THEN 'cancelled'
        WHEN jobs.status = 'succeeded' THEN 'success'
        WHEN jobs.status = 'running' THEN 'running'
        ELSE 'other'
    END as classified_status
FROM attempts_normalized as an
INNER JOIN {{ source('airbyte', 'jobs') }} jobs
ON an.job_id = jobs.id
