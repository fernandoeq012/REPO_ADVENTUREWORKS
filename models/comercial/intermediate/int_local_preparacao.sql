with 
    headerordemdevenda as (
        select *
        from {{ref('stg_erp__salesorderheader')}}
    )

    , endereco as (
        select *
        from {{ref('stg_erp__address')}}
    )

    , estado as (
        select *
        from {{ref('stg_erp__stateprovince')}}
    )

    , pais as (
        select *
        from {{ref('stg_erp__countryregion')}}
    )

    , joined as (
        select
           distinct(headerordemdevenda.fk_endereco_de_envio) as pk_local
           , endereco.cidade as cidade
           , estado.estado as estado
           , pais.pais as pais
        from headerordemdevenda
        left join endereco on headerordemdevenda.fk_endereco_de_envio = endereco.pk_endereco
        left join estado on endereco.fk_estado = estado.pk_estado
        left join pais on estado.fk_pais = pais.pk_pais
    )

select *
from joined
order by pk_local
