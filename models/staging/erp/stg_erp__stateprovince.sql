with
    fonte_stateprovince as (
        select
            STATEPROVINCEID
            , STATEPROVINCECODE
            , COUNTRYREGIONCODE
            , NAME
            , TERRITORYID
        from {{ source('erp_adventureworks', 'STATEPROVINCE') }}
    )

    , renomeacao as (
        select
            cast(STATEPROVINCEID as int) as pk_estado
            , cast(COUNTRYREGIONCODE as varchar(100)) as fk_pais
            , cast(TERRITORYID as int) as fk_territorio
            , cast(STATEPROVINCECODE as varchar(100)) as sigla_estado
            , cast(NAME as varchar(100)) as estado
        from fonte_stateprovince
    )

select *
from renomeacao
