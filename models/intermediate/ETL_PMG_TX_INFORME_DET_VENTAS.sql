with

ventas as (

    select * from {{ ref('ETL_PMG_TX_VENTAS') }}

),

productos as (

    select * from {{ ref('ETL_PMG_TM_PRODUCTO') }}

),

estado as (

    select * from {{ ref('ETL_PMG_DE_ESTADO') }}

),

tiendas as (

    select 
    PK_ANYOMES, 
    PK_ID_TIENDA, 
    PAIS, 
    CIUDAD, 
    ID_GERENTE, 
    ID_JEFE_ZONA, 
    N_EMPLEADOS,
    {{ tipo_tienda('N_EMPLEADOS') }} as TIPO_TIENDA    
    from {{ ref('ETL_PMG_TM_TIENDA') }}

),

impuestos as (

    select * from {{ ref('ETL_PMG_TM_IMPUESTOS') }}

),

max_anyomes_tienda as (

    select 
    tiendas.PK_ANYOMES, 
    tiendas.PK_ID_TIENDA, 
    PAIS, 
    CIUDAD, 
    ID_GERENTE, 
    ID_JEFE_ZONA, 
    N_EMPLEADOS,
    TIPO_TIENDA    
    from tiendas

    inner join (
        select 
        PK_ID_TIENDA, MAX(PK_ANYOMES) as PK_ANYOMES
        from {{ ref('ETL_PMG_TM_TIENDA') }}
        group by PK_ID_TIENDA
        ) max_tienda
    on tiendas.PK_ANYOMES = max_tienda.PK_ANYOMES and tiendas.PK_ID_TIENDA = max_tienda.PK_ID_TIENDA

),

tiendas_impuestos as (

    select 
    max_anyomes_tienda.PK_ANYOMES, 
    max_anyomes_tienda.PK_ID_TIENDA,
    max_anyomes_tienda.PAIS,
    max_anyomes_tienda.CIUDAD,
    max_anyomes_tienda.ID_GERENTE,
    max_anyomes_tienda.ID_JEFE_ZONA,
    max_anyomes_tienda.N_EMPLEADOS,
    max_anyomes_tienda.TIPO_TIENDA,
    I.IMPUESTO
    from max_anyomes_tienda
    left join (
        select 
        case
        when PAIS = 'Estados Unidos' then 'EEUU'
        when PAIS = 'Reino Unido' then 'UK'
        else PAIS end as PAIS,
        ESTADO, 
        CIUDAD, 
        IMPUESTO from {{ ref('ETL_PMG_TM_IMPUESTOS') }}) as I
ON max_anyomes_tienda.PAIS = I.PAIS AND (max_anyomes_tienda.CIUDAD = I.CIUDAD OR (I.CIUDAD IS NULL AND max_anyomes_tienda.PAIS <> "EEUU"))

)

select 
ventas.PK_ID_VENTA,
productos.NOMBRE,
estado.DESCRIPCION,
ventas.FECHA,
tiendas_impuestos.PK_ID_TIENDA,
tiendas_impuestos.TIPO_TIENDA,
tiendas_impuestos.IMPUESTO,
round(productos.PRECIO_UNITARIO,2) as PRECIO_UNITARIO,
round((productos.PRECIO_UNITARIO*ventas.UNIDADES),2) as PRECIO_TOTAL_BRUTO,
round(
-- PRECIO_TOTAL_BRUTO - descuento
((productos.PRECIO_UNITARIO*ventas.UNIDADES)-(productos.PRECIO_UNITARIO*ventas.UNIDADES*ventas.DESCUENTO))
-- Aplicado el impuesto
+ (((((productos.PRECIO_UNITARIO*ventas.UNIDADES)-(productos.PRECIO_UNITARIO*ventas.UNIDADES*ventas.DESCUENTO))))*(tiendas_impuestos.IMPUESTO/100))
,2) as PRECIO_TOTAL_NETO
from ventas

inner join productos
on ventas.ID_PRODUCTO = productos.PK_ID_PRODUCTO

inner join estado
on productos.COD_ESTADO = estado.COD_ESTADO

inner join tiendas_impuestos
on ventas.ID_TIENDA = tiendas_impuestos.PK_ID_TIENDA

