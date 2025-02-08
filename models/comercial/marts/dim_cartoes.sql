with
    int_cartoes as (
        select *
        from {{ ref('int_cartao_preparacao') }}
    )

select *
from int_cartoes