select
    id as order_id,
    user_id as customer_id,
    order_date,
    status

from {{ source('jaffle_shop', 'orders') }}

{{ limit_data_in_devs(column_name='order_date', dev_days_of_data=10000) }}