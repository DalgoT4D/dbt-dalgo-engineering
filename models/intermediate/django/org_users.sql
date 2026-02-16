SELECT 
    ou.org_id,
    au.email as user_email,
    r.slug as role_slug,
    r.name as role_name,
    r.level as role_level
FROM {{ source('django', 'orgusers') }} ou
LEFT JOIN {{ source('django', 'roles') }} r 
    ON ou.new_role_id = r.id
LEFT JOIN {{ source('django', 'users') }} au
    ON ou.user_id = au.id
WHERE ou.org_id IS NOT NULL AND au.email IS NOT NULL AND au.email NOT LIKE '%@projecttech4dev.org' -- remove internal team members