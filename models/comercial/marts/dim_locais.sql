with
    int_locais as (
        select *
        from {{ ref('int_local_preparacao') }}
    )

select *
from int_locais