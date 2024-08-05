{{ config(materialized='view') }}

WITH impuestos AS (
    SELECT
        PAIS,
        ESTADO,
        IMPUESTO
    FROM {{ ref('TM_IMPUESTOS') }}
),
ciudades AS (
    SELECT
        ESTADO,
        CIUDAD
    FROM {{ ref('CIUDADES') }}
)

SELECT
    impuestos.PAIS,
    impuestos.ESTADO,
    ciudades.CIUDAD,
    impuestos.IMPUESTO
FROM
    impuestos
LEFT JOIN
    ciudades
ON
    impuestos.ESTADO = ciudades.ESTADO