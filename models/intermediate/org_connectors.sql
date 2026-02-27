SELECT 
    o.org_id,
    o.org_name,
    o.org_slug,
    o.base_plan,
    o.airbyte_workspace_id,
    a.actor_name,
    a.actor_type,
    a.actor_definition_id,
    a.actor_tombstone,
    a.actor_definition_tombstone,
    a.connector_source,
    a.default_docker_image_tag as workspace_docker_image_tag,
    a.default_docker_image_repository as workspace_docker_image_repository,
    COALESCE(a.current_docker_image_tag, a.default_docker_image_tag) as current_docker_image_tag,
    COALESCE(a.current_docker_image_repository, a.default_docker_image_repository) as current_docker_image_repository,
    a.actor_type as connector_type
FROM {{ ref('all_orgs') }} o
LEFT JOIN {{ ref('actors') }} a
    ON o.airbyte_workspace_id = a.workspace_id
WHERE 
    o.airbyte_workspace_id IS NOT NULL AND 
    a.actor_tombstone is FALSE AND
    a.actor_definition_tombstone is FALSE