select
    /*{{ dbt_utils.surrogate_key(['user_id','order_date'])}} as id,*/
    id as order_id,
    user_id as customer_id,
    order_date,
    date_diff({{ dbt.current_timestamp() }}, order_date, day ) as days_since_ordered,
    status like '%pending%' as is_status_pending,
    case
        when status like '%shipped%' then 'shipped'
        when status like '%return%' then 'returned'
        when status like '%pending%' then 'placed'
        else status
    end as status

from {{ source('jaffle_shop', 'orders') }}

/*{{ limit_data_in_devs(column_name='order_date', dev_days_of_data=10000) }}*/