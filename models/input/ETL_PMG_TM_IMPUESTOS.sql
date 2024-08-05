select 
{{ dbt_utils.surrogate_key(['PAIS','ESTADO','CIUDAD'])}} as PK_ID_PAIS_ESTADO_CIUDAD,
PAIS,
ESTADO,
CIUDAD,
IMPUESTO
from {{ ref('ETL_XXX_TM_IMPUESTOS') }}