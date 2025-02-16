-- 
--    Este teste garante que o total de vendas brutas de 2011 está
--    correto com o valor auditado da contabilidade:
--   $ 12.646.112,16
--

with
    vendas_brutas_2011 as (
        select 
            sum(soma_total_bruto) as total_bruto
        from {{ ref('int_fato_vendas') }}
        where 
            data_do_pedido between '2011-01-01' and '2011-12-31'
    )

select total_bruto
from vendas_brutas_2011
where total_bruto not between 12646112.10 and 12646112.20