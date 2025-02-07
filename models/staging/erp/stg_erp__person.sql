with
    fonte_person as (
        select
        BUSINESSENTITYID
        , FIRSTNAME
        , MIDDLENAME
        , LASTNAME
        , SUFFIX
        from {{ source('erp_adventureworks', 'PERSON') }}
    )

    , renomeacao as (
        select
            cast(BUSINESSENTITYID as int) as pk_negocio
            -- Função coalesce para concatenar FIRSTNAME, MIDDLENAME, LASTNAME e SUFFIX: 
            , COALESCE(FIRSTNAME, '') || 
            CASE WHEN MIDDLENAME IS NOT NULL AND MIDDLENAME <> '' THEN ' ' || MIDDLENAME ELSE '' END || 
            CASE WHEN LASTNAME IS NOT NULL AND LASTNAME <> '' THEN ' ' || LASTNAME ELSE '' END || 
            CASE WHEN SUFFIX IS NOT NULL AND SUFFIX <> '' THEN ' ' || SUFFIX ELSE '' END 
            AS nome_pessoa
        from fonte_person
    )

select *
from renomeacao
