with
    int_clientes as (
        select *
        from {{ ref('int_clientes_preparacao') }}
    )

select *
from int_clientes