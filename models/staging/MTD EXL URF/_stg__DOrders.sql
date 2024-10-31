with source as (
      select * from {{ source('PROD_RAW', 'VW_D_ORDERS') }}
)
select 
ORDER_KEY,
DIVISION,
QUOTE_ORDER_NUMBER
from source
GROUP BY 1,2,3
