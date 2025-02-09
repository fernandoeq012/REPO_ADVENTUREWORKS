with 
    motivovenda as (
        select *
        from {{ref('stg_erp__salesreason')}}
    )

    , headermotivovenda as (
        select *
        from {{ref('stg_erp__salesorderheadersalesreason')}}
    )

    , joined as (
        select
           headermotivovenda.fk_ordem_de_venda as id_venda
           , motivovenda.motivo as motivo
        from headermotivovenda
        left join motivovenda on headermotivovenda.fk_motivo = motivovenda.pk_motivo
    )

select *
from joined
order by id_venda