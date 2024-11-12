with source as (
      select * from {{ source('OPG_PL', 'ENT_MAP') }}
)
select * from source
  