with source as (
      select * from {{ source('DEV_AIDA', 'DimCALENDAR') }}
)

select *
{# "Date",
C_YEAR,
C_WEEK_NBR,
F_WEEK,
F_WEEK_START_DATE,
F_YEAR #}
FROM source
