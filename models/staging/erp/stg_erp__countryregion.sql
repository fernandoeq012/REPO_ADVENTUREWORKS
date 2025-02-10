with
    fonte_countryregion as (
        select
            COUNTRYREGIONCODE
            , NAME
        from {{ source('erp_adventureworks', 'COUNTRYREGION') }}
    )

    , renomeacao as (
        select
            cast(COUNTRYREGIONCODE as varchar) as pk_pais
            , cast(NAME as varchar) as pais
        from fonte_countryregion
    )

select *
from renomeacao
