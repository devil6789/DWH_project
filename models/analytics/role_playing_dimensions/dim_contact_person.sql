WITH contact_person__source AS (
    SELECT *
    FROM {{ ref("dim_person") }}
)

, contact_person__rename AS (
    SELECT
        person_key AS contact_person_key
        , full_name AS contact_full_name
        , search_name AS contact_search_name
        , is_permitted_to_logon
        , is_external_logon_provider
        , is_system_user
        , is_employee
        , is_salesperson
    FROM `contact_person__source`
)

    SELECT
        contact_person_key
        , contact_full_name
        , contact_search_name
        , is_permitted_to_logon
        , is_external_logon_provider
        , is_system_user
        , is_employee
        , is_salesperson
    FROM `contact_person__rename`
    