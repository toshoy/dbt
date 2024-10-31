with source as (
      select * from {{ source('PROD_RAW', 'VW_D_WAREHOUSE') }}
)
select 
WAREHOUSE_KEY,
CORP_WAREHOUSE_CODE
from source
GROUP BY 1,2