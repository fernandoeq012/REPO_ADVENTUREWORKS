with
    int_produtos as (
        select *
        from {{ ref('int_produto_preparacao') }}
    )

select *
from int_produtos