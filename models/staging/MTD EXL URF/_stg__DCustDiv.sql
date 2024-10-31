with source as (
      select * from {{ source('PROD_RAW', 'VW_D_CUSTBYDIV') }}
)
select 
customer_key,
division,
demand_group
from source
where division <> '30'
group by all