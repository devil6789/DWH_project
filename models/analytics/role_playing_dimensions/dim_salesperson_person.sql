WITH salesperson_person__source AS (
    SELECT *
    FROM {{ ref("dim_person") }}
)

, salesperson_person__rename AS (
    SELECT
        person_key AS salesperson_person_key
        , full_name AS salesperson_full_name
        , search_name AS salesperson_search_name
        , is_permitted_to_logon
        , is_external_logon_provider
        , is_system_user
        , is_employee
        , is_salesperson
    FROM `salesperson_person__source`
)

    SELECT
        salesperson_person_key
        , salesperson_full_name
        , salesperson_search_name
        , is_permitted_to_logon
        , is_external_logon_provider
        , is_system_user
        , is_employee
        , is_salesperson
    FROM `salesperson_person__rename`
    WHERE is_salesperson = 'Sales Person'
    OR    salesperson_person_key IN (-1,0)