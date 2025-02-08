with
    fonte_salesorderheader as (
        select
            SALESORDERID
            , ORDERDATE
            , STATUS
            , PURCHASEORDERNUMBER
            , CUSTOMERID
            , SALESPERSONID
            , SHIPTOADDRESSID
            , CREDITCARDID
            , TOTALDUE
        from {{ source('erp_adventureworks', 'SALESORDERHEADER') }}
    )

    , renomeacao as (
        select
            cast(SALESORDERID as int) as pk_ordem_de_venda
            , cast(CUSTOMERID as int) as fk_cliente
            , cast(SALESPERSONID as int) as fk_vendedor
            , cast(SHIPTOADDRESSID as int) as fk_endereco_de_envio
            , cast(CREDITCARDID as int) as fk_cartao
            , cast(TOTALDUE as float) as total_da_compra
            , cast(STATUS as int) as status_do_pedido
            , cast(PURCHASEORDERNUMBER as varchar) as numero_da_compra
            , cast(ORDERDATE as date) as data_do_pedido
        from fonte_salesorderheader
    )

select *
from renomeacao
