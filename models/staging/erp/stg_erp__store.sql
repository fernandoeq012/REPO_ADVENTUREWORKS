with
    fonte_store as (
        select
        BUSINESSENTITYID
        , SALESPERSONID
        , NAME
        from {{ source('erp_adventureworks', 'STORE') }}
    )

    , renomeacao as (
        select
            cast(BUSINESSENTITYID as int) as pk_negocio
            , cast(SALESPERSONID as int) as fk_vendedor
            , cast(NAME as varchar) as nome_loja
        from fonte_store
    )

select *
from renomeacao
