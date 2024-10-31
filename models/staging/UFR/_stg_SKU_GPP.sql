with source as (
      select * from {{ source('DEV_AIDA', 'TEST_DimCON_SKU') }}
)
select 
SKU_KEY,
MIN(GPP_KEY) AS "GPP_KEY",
MIN(BRAND_KEY) AS "BRAND_KEY"
FROM source
GROUP BY ALL