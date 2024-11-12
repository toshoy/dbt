with source as (
      select * from {{ source('DEV_AIDA', 'TESTCR') }}
)
select * from source
  