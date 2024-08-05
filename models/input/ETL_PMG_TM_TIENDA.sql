select 
{{ dbt_utils.surrogate_key(['PK_ANYOMES','PK_ID_TIENDA'])}} as PK_ID_TIENDA_AND_ANYOMES,
PK_ANYOMES,
PK_ID_TIENDA,
PAIS,
CIUDAD,
ID_GERENTE,
ID_JEFE_ZONA,
N_EMPLEADOS
from {{ source('dbt_pmilaguasch', 'ETL_XXX_TM_TIENDA') }}