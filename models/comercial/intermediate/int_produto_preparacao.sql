with 
    detalhepedidovenda as (
        select *
        from {{ref('stg_erp__salesorderdetail')}}
    )

    , produto as (
        select *
        from {{ref('stg_erp__product')}}
    )

    , joined as (
        select
           distinct(detalhepedidovenda.fk_produto) as pk_produto
           , produto.nome_produto as produto
        from detalhepedidovenda
        left join produto on detalhepedidovenda.fk_produto = produto.pk_produto
    )

select *
from joined
order by pk_produto