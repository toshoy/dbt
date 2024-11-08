with source as (
      select * from {{ source('DEV_AIDA', 'DimCON_PRODUCT_GPP') }}
)
select * from source