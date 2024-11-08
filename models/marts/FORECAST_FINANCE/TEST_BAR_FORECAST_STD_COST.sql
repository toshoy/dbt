with demand as (
    select *
    from {{ ref('_stg__Forecast_demand') }}
),
std_cost_gpp as (
    select 
    dmd.*,
    std.UNIT_STD_MAT_COST,
    std."%_FREIGHT",
    gpp.GPP_DIVISION_CODE
    from demand AS dmd
    left join {{ ref('TEST_BAR_STD_COST_FREIGHT') }}  AS std ON dmd.SKU_KEY = std.PRODUCT_MATERIAL_CODE
    left join {{ ref('_stg_DIM_GPP') }} as gpp on dmd.GPP_KEY = gpp.GPP_KEY
    left join {{ ref('_stg_Dim_Con_Market_channel') }} as mrkt on mrkt.mrkt_chnl_key = dmd.mrkt_chnl_key
    )
SELECT *
FROM std_cost_gpp