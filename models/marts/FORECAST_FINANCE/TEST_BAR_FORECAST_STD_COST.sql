with demand as (
    select *
    from {{ ref('_stg__Forecast_demand') }}
),
std_cost_gpp as (
    select 
    dmd.*,
    std.UNIT_STD_MAT_COST,
    std."%_FREIGHT",
    gpp.GPP_DIVISION_CODE,
    mrkt.REGION,
    mrkt."MARKET CHANNEL",
    mrkt."ROC Dmd Grp"
    from demand AS dmd
    left join {{ ref('TEST_BAR_STD_COST_FREIGHT') }}  AS std ON dmd.SKU_KEY = std.PRODUCT_MATERIAL_CODE
    left join {{ ref('_stg_DIM_GPP') }} as gpp on dmd.GPP_KEY = gpp.GPP_KEY
    left join {{ ref('_stg_Dim_Con_Market_channel') }} as mrkt on mrkt.mrkt_chnl_key = dmd.mrkt_chnl_key
    ),
bar_key as (
    select *,
    IFF( 
        "REGION" = 'NORTH AMERICA', 
        UPPER((CONCAT(COALESCE(TRIM("GPP_DIVISION_CODE"),''), COALESCE(TRIM("MARKET CHANNEL"),'')))),
        UPPER(CONCAT(COALESCE(TRIM("GPP_DIVISION_CODE"),''), COALESCE(TRIM("ROC Dmd Grp"),'')))) AS "BAR_KEY"
    from std_cost_gpp),
pnl_join as (
    select dmd.*,
    pnl."RSA_%_{{var('rolling_avg')}}_MONTHS",
    pnl."TOTAL_GTN_%_{{var('rolling_avg')}}_MONTHS"
    from bar_key as dmd
    left join {{ ref('_2_rolling_avg') }} as pnl on dmd.bar_key = pnl.bar_key )
SELECT *
FROM pnl_join