with source as (
      select * from {{ source('PROD_RAW', 'VW_D_DATES') }}
)
select 
DATE_KEY,
CAL_DATE,
TO_DATE("CAL_DATE",'DD-MON-YY') as "DATE"
from source
