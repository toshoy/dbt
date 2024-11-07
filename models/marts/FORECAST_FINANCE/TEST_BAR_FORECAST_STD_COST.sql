with demand as (
    select *
    from {{ ref('_stg__Forecast_demand') }}
),
std_cost as (
    select 
    dmd.*,
    std.UNIT_STD_MAT_COST,
    std."%_FREIGHT"
    from demand AS dmd
    left join {{ ref('TEST_BAR_STD_COST_FREIGHT') }}  AS std ON dmd.SKU_KEY = std.PRODUCT_MATERIAL_CODE
)
SELECT *
FROM std_cost