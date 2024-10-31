with source as (
      select * from {{ source('DEV_AIDA', 'LOOKUP_MTDEXL_MRKT_CHNL_KEY') }}
)
select 
customer_number,
division,
mrkt_chnl_key
from source
where exclude NOT LIKE '%EXCLUDE%' OR "EXCLUDE" IS NULL