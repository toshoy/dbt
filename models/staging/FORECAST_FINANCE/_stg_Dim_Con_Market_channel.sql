with source as (
      select * from {{ source('DEV_AIDA', 'DimCON_MARKET_CHANNEL') }}
)
select * from source