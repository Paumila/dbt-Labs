{#

{% snapshot snapshot_orders %}

{{
    config(
      target_schema='dbt_snapshot',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}

#}