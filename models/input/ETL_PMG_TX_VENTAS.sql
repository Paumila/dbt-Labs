select * from {{ source('dbt_pmilaguasch', 'ETL_XXX_TX_VENTAS') }}