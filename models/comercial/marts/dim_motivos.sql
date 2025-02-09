with
    int_motivos as (
        select *
        from {{ ref('int_motivo_preparacao') }}
    )

select *
from int_motivos