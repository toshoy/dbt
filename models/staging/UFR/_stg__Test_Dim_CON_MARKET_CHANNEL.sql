with source as (
      select * from {{ source('DEV_AIDA', 'TEST_DimCON_MARKET_CHANNEL') }}
)
select * from source
  