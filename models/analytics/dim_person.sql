WITH dim_person__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename AS (
    SELECT 
        person_id AS person_key
        , full_name
        , preferred_name
        , search_name
        , is_permitted_to_logon AS is_permitted_to_logon_boolean
        , is_external_logon_provider AS is_external_logon_provider_boolean
        , is_system_user AS is_system_user_boolean
        , is_employee AS is_employee_boolean
        , is_salesperson AS is_salesperson_boolean
    FROM `dim_person__source`
)

, dim_person__cast_type AS (
    SELECT 
        CAST(person_key AS INT) AS person_key
        , CAST(full_name AS STRING) AS full_name
        , CAST(preferred_name AS STRING) AS preferred_name
        , CAST(search_name AS STRING) AS search_name
        , CAST(is_permitted_to_logon_boolean AS BOOLEAN) AS is_permitted_to_logon_boolean
        , CAST(is_external_logon_provider_boolean AS BOOLEAN) AS is_external_logon_provider_boolean
        , CAST(is_system_user_boolean AS BOOLEAN) AS is_system_user_boolean
        , CAST(is_employee_boolean AS BOOLEAN) AS is_employee_boolean
        , CAST(is_salesperson_boolean AS BOOLEAN) AS is_salesperson_boolean
    FROM `dim_person__rename`
)

, dim_person__handle_boolean AS (
    SELECT
        person_key
        , full_name
        , preferred_name
        , search_name
        , CASE
            WHEN is_permitted_to_logon_boolean IS TRUE THEN 'Permitted To Logon'
            WHEN is_permitted_to_logon_boolean IS FALSE THEN 'Not Permitted To Logon'
            WHEN is_permitted_to_logon_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_permitted_to_logon
        , CASE
            WHEN is_external_logon_provider_boolean IS TRUE THEN 'External Logon Provider'
            WHEN is_external_logon_provider_boolean IS FALSE THEN 'Not External Logon Provider'
            WHEN is_external_logon_provider_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_external_logon_provider
        , CASE
            WHEN is_system_user_boolean IS TRUE THEN 'System User'
            WHEN is_system_user_boolean IS FALSE THEN 'Not System User'
            WHEN is_system_user_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_system_user
        , CASE
            WHEN is_employee_boolean IS TRUE THEN 'Employee'
            WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
            WHEN is_employee_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_employee
        , CASE
            WHEN is_salesperson_boolean IS TRUE THEN 'Sales Person'
            WHEN is_salesperson_boolean IS FALSE THEN 'Not Sales Person'
            WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_salesperson
    FROM `dim_person__cast_type`
)

SELECT * FROM `dim_person__handle_boolean`

