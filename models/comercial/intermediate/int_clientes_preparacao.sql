with 
    clientes as (
        select *
        from {{ref('stg_erp__customer')}}
    )

    , pessoas as (
        select *
        from {{ref('stg_erp__person')}}
    )

    , lojas as (
        select *
        from {{ref('stg_erp__store')}}
    )

    , joined as (
        select
           clientes.pk_cliente as pk_cliente
           , pessoas.pk_negocio as fk_pessoa
           , lojas.pk_negocio as fk_loja
           , pessoas.nome_pessoa as nome_pessoa
           , lojas.nome_loja as nome_loja
           -- Quando as colunas personid e storeid estiverem preenchidas na tabela customer, será escolhido
           -- como cliente o nome da pessoa.
           , case
                when pessoas.nome_pessoa is null
                then lojas.nome_loja
                else pessoas.nome_pessoa
                end as cliente
        from clientes
        left join pessoas on clientes.fk_pessoa = pessoas.pk_negocio
        left join lojas on clientes.fk_loja = lojas.pk_negocio
        order by clientes.pk_cliente
    )

select *
from joined