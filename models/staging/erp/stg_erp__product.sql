with
    fonte_product as (
        select
            PRODUCTID
            , PRODUCTNUMBER
            , NAME
        from {{ source('erp_adventureworks', 'PRODUCT') }}
    )

    , renomeacao as (
        select
            cast(PRODUCTID as int) as pk_produto
            , cast(PRODUCTNUMBER as varchar(100)) as numero_produto
            , cast(NAME as varchar(100)) as nome_produto
        from fonte_product
    )

select *
from renomeacao
