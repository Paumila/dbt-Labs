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
    {{ tipo_tienda(N_EMPLEADOS) }} as TIPO_TIENDA    
    from {{ ref('ETL_PMG_TM_TIENDA') }}

),

impuestos as (

    select * from {{ ref(' ETL_PMG_TM_IMPUESTOS') }}

),

max_anyomes_tienda as (

    select 
    PK_ANYOMES, 
    PK_ID_TIENDA, 
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
        group by PK_ID_TIENDA
        from {{ ref('ETL_PMG_TM_TIENDA') }}
        ) max_tienda
    on tiendas.PK_ANYOMES = max_tienda.PK_ANYOMES and tiendas.PK_ID_TIENDA = max_tienda.PK_ID_TIENDA

),

tiendas_impuestos as (

    select 
    T.PK_ANYOMES, 
    T.PK_ID_TIENDA,
    T.PAIS,
    T.CIUDAD,
    T.ID_GERENTE,
    T.ID_JEFE_ZONA,
    T.N_EMPLEADOS,
    T.TIPO_TIENDA,
    I.IMPUESTO
    from max_anyomes_tienda as T
    left join (
        select 
        case
        when PAIS = 'Estados Unidos' then 'EEUU'
        when PAIS = 'Reino Unido' then 'UK'
        else PAIS end as PAIS,
        ESTADO, 
        CIUDAD, 
        IMPUESTO FROM {{ ref('ETL_PMG_TM_IMPUESTOS') }}) as I
ON T.PAIS = I.PAIS AND (T.CIUDAD = I.CIUDAD OR (I.CIUDAD IS NULL AND T.PAIS <> "EEUU"))

)

select 
ventas.*,
productos.*,
estado.*,
tiendas_impuestos.*
from ventas

inner join productos
on ventas.ID_PRODUCTO = productos.PK_ID_PRODUCTO

inner join estado
on productos.COD_ESTADO = estado.COD_ESTADO

inner join tiendas_impuestos
on ventas.ID_TIENDA = tiendas_impuestos.PK_ID_TIENDA

