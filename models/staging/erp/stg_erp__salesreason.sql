with
    fonte_salesreason as (
        select
            SALESREASONID
            , NAME
        from {{ source('erp_adventureworks', 'SALESREASON') }}
    )

    , renomeacao as (
        select
            cast(SALESREASONID as int) as pk_motivo
            , cast(NAME as varchar) as motivo
        from fonte_salesreason
    )

select *
from renomeacao
