with source as (
      select * from {{ source('PROD_RAW', 'VW_D_ITEMS') }}
)
select 
ITEM_KEY,
ITEM_NUMBER,
ITEM_DESCRIPTION,
ITEM_TYPE_DESCRIPTION
from source