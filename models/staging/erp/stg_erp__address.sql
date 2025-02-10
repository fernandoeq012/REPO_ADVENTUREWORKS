with
    fonte_address as (
        select
            ADDRESSID
            , CITY
            , STATEPROVINCEID
        from {{ source('erp_adventureworks', 'ADDRESS') }}
    )

    , renomeacao as (
        select
            cast(ADDRESSID as int) as pk_endereco
            , cast(STATEPROVINCEID as int) as fk_estado
            , cast(CITY as varchar) as cidade
        from fonte_address
    )

select *
from renomeacao
