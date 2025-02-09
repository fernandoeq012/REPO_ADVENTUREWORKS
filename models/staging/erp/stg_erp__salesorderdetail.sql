with
    fonte_salesorderdetail as (
        select
            SALESORDERID
            , SALESORDERDETAILID
            , ORDERQTY
            , PRODUCTID
            , UNITPRICE
            , UNITPRICEDISCOUNT
        from {{ source('erp_adventureworks', 'SALESORDERDETAIL') }}
    )

    , renomeacao as (
        select
            cast(SALESORDERDETAILID as int) as pk_detalhe_ordem_venda
            , cast(SALESORDERID as int) as fk_ordem_de_venda
            , cast(ORDERQTY as int) as quantidade
            , cast(PRODUCTID as int) as fk_produto
            , cast(UNITPRICE as float) as preco_unitario
            , cast(UNITPRICEDISCOUNT as float) as desconto_preco_unitario
        from fonte_salesorderdetail
    )

select *
from renomeacao
