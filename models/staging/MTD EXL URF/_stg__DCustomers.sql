with source as (
      select * from {{ source('PROD_RAW', 'VW_D_CUSTOMERS') }}
)
select 
customer_key,
customer_number,
custmer_name as "CUSTOMER_NAME" ,
entity_group,
entity_group_name
from source
where inter_company = 'N'
group by all
