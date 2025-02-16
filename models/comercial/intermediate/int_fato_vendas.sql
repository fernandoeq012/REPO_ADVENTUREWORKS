with
    detalhepedidovendas as (
        select * 
        from {{ ref('stg_erp__salesorderdetail') }}
    )

    , headerpedidovendas as (
        select * 
        from {{ ref('stg_erp__salesorderheader') }}
    )

    , headermotivovendas as (
        select * 
        from {{ ref('stg_erp__salesorderheadersalesreason') }}
    )

    , motivovendas as (
        select * 
        from {{ ref('stg_erp__salesreason') }}
    )

    , motivos_agrupados as (
        select
            fk_ordem_de_venda
            , listagg(motivovendas.motivo, ', ') within group (order by motivovendas.motivo) as motivos_de_venda
        from headermotivovendas
        left join motivovendas on headermotivovendas.fk_motivo = motivovendas.pk_motivo
        group by fk_ordem_de_venda
    )

    , joined as (
        select
            MD5(concat(headerpedidovendas.pk_ordem_de_venda, '/', detalhepedidovendas.pk_detalhe_ordem_venda)) as sk_vendas
            , detalhepedidovendas.pk_detalhe_ordem_venda as pk_vendas
            , headerpedidovendas.pk_ordem_de_venda  
            , detalhepedidovendas.fk_produto
            , headerpedidovendas.fk_endereco_de_envio
            , headerpedidovendas.fk_vendedor
            , headerpedidovendas.fk_cliente
            , headerpedidovendas.fk_cartao
            , headerpedidovendas.data_do_pedido
            , headerpedidovendas.data_do_envio
            , case headerpedidovendas.status_do_pedido
                when 1 then 'In process'
                when 2 then 'Approved'
                when 3 then 'Backordered'
                when 4 then 'Rejected'
                when 5 then 'Shipped'
                when 6 then 'Cancelled'
                else 'Unknown'
            end as status_do_pedido
            , coalesce(motivos_agrupados.motivos_de_venda, 'Not specified') as motivo
            , headerpedidovendas.eh_online
            , detalhepedidovendas.quantidade
            , detalhepedidovendas.preco_unitario
            , detalhepedidovendas.desconto_preco_unitario
            , headerpedidovendas.imposto
            , headerpedidovendas.frete
            , headerpedidovendas.subtotal
            , headerpedidovendas.total_da_compra
            , (detalhepedidovendas.preco_unitario * detalhepedidovendas.quantidade) as valor_bruto_negociado
            , sum(detalhepedidovendas.preco_unitario * detalhepedidovendas.quantidade) 
                over (partition by sk_vendas) as soma_total_bruto
            , (detalhepedidovendas.preco_unitario * detalhepedidovendas.quantidade * 
                (1 - coalesce(detalhepedidovendas.desconto_preco_unitario, 0))) as valor_liquido_negociado
            , sum(detalhepedidovendas.preco_unitario * detalhepedidovendas.quantidade) 
                over (partition by detalhepedidovendas.fk_produto) as venda_bruta_produto
            , cast(
                (sum(detalhepedidovendas.preco_unitario * detalhepedidovendas.quantidade * 
                    (1 - coalesce(detalhepedidovendas.desconto_preco_unitario, 0))) 
                over (partition by detalhepedidovendas.fk_produto)) / 
                nullif(count(detalhepedidovendas.fk_produto) over (partition by detalhepedidovendas.fk_produto), 0)
                as numeric(18,2)
            ) as ticket_medio

        from headerpedidovendas
        left join detalhepedidovendas on headerpedidovendas.pk_ordem_de_venda = detalhepedidovendas.fk_ordem_de_venda
        left join motivos_agrupados on headerpedidovendas.pk_ordem_de_venda = motivos_agrupados.fk_ordem_de_venda
    )

select * 
from joined 
order by pk_vendas
