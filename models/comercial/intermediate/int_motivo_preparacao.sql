with 
    motivovenda as (
        select *
        from {{ref('stg_erp__salesreason')}}
    )

    , motivopreparado as (
        select
           motivovenda.pk_motivo as pk_motivo
           , motivovenda.motivo as motivo
        from motivovenda
    )

select *
from motivopreparado
order by pk_motivo