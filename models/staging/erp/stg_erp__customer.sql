with
    fonte_customer as (
        select
            CUSTOMERID
            , PERSONID
            , STOREID
            , TERRITORYID
        from {{ source('erp_adventureworks', 'CUSTOMER') }}
    )

    , renomeacao as (
        select
            cast(CUSTOMERID as int) as pk_cliente
            , cast(PERSONID as int) as fk_pessoa
            , cast(STOREID as int) as fk_loja
            , cast(TERRITORYID as int) as fk_territorio
        from fonte_customer
    )

select *
from renomeacao
