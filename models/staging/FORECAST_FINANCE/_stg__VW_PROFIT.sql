with source as (
      select * from {{ source('OPG_PL', 'VW_PROFIT') }}
)
select *
from source