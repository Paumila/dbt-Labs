
{{ config(severity = 'warn') }}

SELECT *
FROM
    {{ ref('ETL_PMG_TX_INFORME_DET_VENTAS') }}
WHERE
    DESCRIPCION = 'DESCATALOGADO'