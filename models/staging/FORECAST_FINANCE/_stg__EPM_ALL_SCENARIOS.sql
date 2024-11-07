with source as (
      select * from {{ source('TEST_MARTS', 'VW_EPM_ALL_SCENARIOS') }}
)
select * from source
  