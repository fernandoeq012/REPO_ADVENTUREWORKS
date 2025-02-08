with
    fonte_creditcard as (
        select
            CREDITCARDID
            , CARDTYPE
        from {{ source('erp_adventureworks', 'CREDITCARD') }}
    )

    , renomeacao as (
        select
            cast(CREDITCARDID as int) as pk_cartao
            , cast(CARDTYPE as varchar) as tipo_cartao
        from fonte_creditcard
    )

select *
from renomeacao
