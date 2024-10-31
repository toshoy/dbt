with source as (
      select * from {{ source('DEV_AIDA', 'TEST_Support_Facts_NPD_SKU_Incr') }}
)
select * from source
  