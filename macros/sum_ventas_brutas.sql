{% macro test_sum_ventas_brutas() %}

WITH
ventas_brutas_aggregated AS (
    SELECT 
        ROUND(SUM(PRECIO_TOTAL_BRUTO),2) AS sum_bruto_aggregated
    FROM 
        {{ ref('ETL_PMG_TX_INFORME_DET_VENTAS') }}
    
),

ventas_brutas_initial AS (
    SELECT 
        ROUND(SUM(V.UNIDADES * P.PRECIO_UNITARIO),2) AS sum_bruto_initial
    FROM 
        {{ ref('ETL_PMG_TX_VENTAS') }} V
    INNER JOIN 
        {{ ref('ETL_PMG_TM_PRODUCTO') }} P
    ON 
        V.ID_PRODUCTO = P.PK_ID_PRODUCTO
)

SELECT
    ventas_brutas_aggregated.sum_bruto_aggregated,
    ventas_brutas_initial.sum_bruto_initial
FROM
    ventas_brutas_aggregated,
    ventas_brutas_initial
WHERE
    ventas_brutas_aggregated.sum_bruto_aggregated != ventas_brutas_initial.sum_bruto_initial

{% endmacro %}