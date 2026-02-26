SELECT 
    actor.workspace_id,
    actor.name as actor_name,
    actor.actor_type,
    actor.actor_definition_id,
    actor.tombstone as actor_tombstone,
    actor_definition.default_version_id,
    actor_definition.tombstone as actor_definition_tombstone,
    CASE 
        WHEN actor_definition.custom THEN 'custom' 
        ELSE 'official' 
    END as connector_source,
    default_actor_definition_version.id as default_actor_definition_version_id,
    default_actor_definition_version.docker_image_tag as default_docker_image_tag,
    default_actor_definition_version.docker_repository as default_docker_image_repository,
    default_actor_definition_version.cdk_version as default_cdk_version,
    current_actor_definition_version.id as current_actor_definition_version_id,
    current_actor_definition_version.docker_image_tag as current_docker_image_tag,
    current_actor_definition_version.docker_repository as current_docker_image_repository,
    current_actor_definition_version.cdk_version as current_cdk_version
FROM {{ source('airbyte', 'actor') }}
INNER JOIN {{ source('airbyte', 'actor_definition') }}
    ON actor.actor_definition_id = actor_definition.id
INNER JOIN {{ source('airbyte', 'actor_definition_version') }} as default_actor_definition_version
    ON actor_definition.default_version_id = default_actor_definition_version.id
LEFT JOIN {{ source('airbyte', 'scoped_configuration') }}
    ON scoped_configuration.scope_id = actor.id
LEFT JOIN {{ source('airbyte', 'actor_definition_version') }} as current_actor_definition_version
    ON CAST(current_actor_definition_version.id as CHARACTER VARYING) = scoped_configuration."value"