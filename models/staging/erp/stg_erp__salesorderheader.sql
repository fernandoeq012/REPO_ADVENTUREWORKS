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
            , SHIPDATE
            , ONLINEORDERFLAG
            , SUBTOTAL
            , TAXAMT
            , FREIGHT
        from {{ source('erp_adventureworks', 'SALESORDERHEADER') }}
    )

    , renomeacao as (
        select
            cast(SALESORDERID as int) as pk_ordem_de_venda
            , cast(CUSTOMERID as int) as fk_cliente
            , cast(SALESPERSONID as int) as fk_vendedor
            , cast(SHIPTOADDRESSID as int) as fk_endereco_de_envio
            , cast(CREDITCARDID as int) as fk_cartao
            , cast(TOTALDUE as numeric(20,4)) as total_da_compra
            , cast(STATUS as int) as status_do_pedido
            , cast(PURCHASEORDERNUMBER as varchar) as numero_da_compra
            , cast(FREIGHT as numeric(20,4)) as frete
            , cast(TAXAMT as numeric(20,4)) as imposto
            , cast(SUBTOTAL as numeric(20,4)) as subtotal
            , cast(ONLINEORDERFLAG as int) as eh_online
            , cast(SHIPDATE as date) as data_do_envio
            , cast(ORDERDATE as date) as data_do_pedido
        from fonte_salesorderheader
    )

select *
from renomeacao
