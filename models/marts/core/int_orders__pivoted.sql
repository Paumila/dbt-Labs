{%- set payment_method = ['bank_transfer','coupon','credit_card','gift_card'] -%}

with payments as (
    select * from {{ ref('stg_stripe_payments') }}
),

/*
pivoted as (
    select 
    order_id,
    sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
    sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
    sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
    sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount 
    from payments
    where status = 'success'
    group by 1
    order by 1 asc
)
*/

pivoted as (
    select 
    order_id,
    {%- for payment_method in payment_method %}
    sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
    {%- if not loop.last -%}
        ,
    {%- endif -%}
    {% endfor %}
    from payments
    where status = 'success'
    group by 1
    order by 1 asc
)

select * from pivoted