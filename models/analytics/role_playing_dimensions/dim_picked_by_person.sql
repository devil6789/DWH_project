WITH picked_by_person__source AS (
    SELECT *
    FROM `learn-dwh-411512.wide_world_importers_dwh.dim_person`
)

, picked_by_person__rename AS (
    SELECT
        person_key AS picked_by_person_key
        , full_name AS picked_by_full_name
        , search_name AS picked_by_search_name
        , is_permitted_to_logon
        , is_external_logon_provider
        , is_system_user
        , is_employee
        , is_salesperson
    FROM `picked_by_person__source`
)

    SELECT
        picked_by_person_key
        , picked_by_full_name
        , picked_by_search_name
        , is_permitted_to_logon
        , is_external_logon_provider
        , is_system_user
        , is_employee
        , is_salesperson
    FROM `picked_by_person__rename`
    