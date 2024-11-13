with source as (
      select * from {{ source('PROD_RAW', 'DCT_BAR_ENTITY_ATTR') }}
)
select * from source
