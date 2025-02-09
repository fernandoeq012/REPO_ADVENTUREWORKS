with 
    headerpedidovenda as (
        select *
        from {{ref('stg_erp__salesorderheader')}}
    )

    , cartao as (
        select *
        from {{ref('stg_erp__creditcard')}}
    )

    , joined as (
        select
           distinct(headerpedidovenda.fk_cartao) as pk_cartao
           , cartao.tipo_cartao as cartao
        from headerpedidovenda
        left join cartao on headerpedidovenda.fk_cartao = cartao.pk_cartao
        where pk_cartao is not null
    )

select *
from joined
order by pk_cartao