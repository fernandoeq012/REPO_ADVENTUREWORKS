with
    fonte_salesorderheadersalesreason as (
        select
            SALESORDERID
            , SALESREASONID
        from {{ source('erp_adventureworks', 'SALESORDERHEADERSALESREASON') }}
    )

    , renomeacao as (
        select
            concat(SALESORDERID, '/', SALESREASONID) as pk_motivo_de_venda
            , cast(SALESORDERID as int) as fk_ordem_de_venda
            , cast(SALESREASONID as int) as fk_motivo
        from fonte_salesorderheadersalesreason
    )

select *
from renomeacao