SELECT 
    wq.id as work_queue_id,
    wq.name as work_queue_name,
    wq.concurrency_limit as work_queue_concurrency_limit,
    wp.name as work_pool_name,
    wp.type as work_pool_type,
    wp.concurrency_limit as work_pool_concurrency_limit
FROM {{ source('prefect', 'work_queue') }} wq
LEFT JOIN {{ source('prefect', 'work_pool') }} wp 
    ON wq.work_pool_id = wp.id