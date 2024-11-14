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
    pnl."DaA_%_{{var('rolling_avg')}}_MONTHS",
    pnl."FILL_RATE_%_{{var('rolling_avg')}}_MONTHS",
    pnl."GTN_OTHER_%_{{var('rolling_avg')}}_MONTHS",
    pnl."RSA_%_{{var('rolling_avg')}}_MONTHS",
    pnl."TOTAL_GTN_%_{{var('rolling_avg')}}_MONTHS"
    from bar_key as dmd
    left join {{ ref('_2_rolling_avg') }} as pnl on dmd.bar_key = pnl.BAR_KEY_ADJ ),
final as (
    select
    "SRC_SYS_KEY",
    "CURR_RCRD_FLAG",
    "DMD_FCST_CTGY",
    "MRKT_CHNL_KEY",
    "SKU_KEY",
    "LOC_KEY",
    "FCST_DUR",
    "FCST_ID_TYP",
    "FCST_TYP",
    "FCST_START_DTE"::DATE AS "FCST_START_DTE",
    "PLNG_MODEL_NAME",
    "F_MONTH_DATE",
    "ETL_INS_DTE",
    "PRC_PRFL_UNIT_GSV",
    "DMD_GRP_UNIT_GSV",
    "DMD_GRP_ROLLUP_UNIT_GSV",
    "DMD_GRP_BUS_REGN_UNIT_GSV",
    "HIST_START_DATE",
    "HIST_END_DATE",
    "HIST_UNIT_GSV",
    "Avg_CRNCY_RECENT_PRICE",
    "RECENT_PRC_PRFL_UNIT_GSV",
    "HIST_AND_RCNT_PRICE_UNIT_GSV",
    "FCST_QTY",
    "FCST_GSV",
    ROUND("UNIT_STD_MAT_COST"*"FCST_QTY",2) AS "COS_STD_COST",
    ROUND("%_FREIGHT"*"FCST_GSV",2) AS "COS_OB_FREIGHT",
    ROUND("RSA_%_-6_MONTHS"*"FCST_GSV",2)*-1 AS "RSA",
    ROUND("FILL_RATE_%_-6_MONTHS"*"FCST_GSV",2) AS "GTN_FILL_RATE_FINES",
    ROUND("DaA_%_-6_MONTHS"*"FCST_GSV",2) AS "GTN_DISC_ALLOW",
    ROUND("GTN_OTHER_%_-6_MONTHS"*"FCST_GSV",2) AS "GTN_OTHER",
    ROUND("TOTAL_GTN_%_-6_MONTHS"*"FCST_GSV",2) AS "GTN_TOTAL",
    "RUN_DATE",
    "DMD_GRP_TYP",
    "PLNG_LVL",
    "DFU_LVL",
    "SRC_SF_TBL_KEY",
    "PROD_KEY",
    "PROGRAM ID",
    "Yearly Outlook",
    "FUZZY_FLAG",
    "GPP_KEY",
    "BRAND_KEY",
    "UNIT_STD_MAT_COST",
    "%_FREIGHT" AS "OB_FREIGHT_%",
    "GPP_DIVISION_CODE",
    "REGION",
    "MARKET CHANNEL",
    "ROC Dmd Grp",
    "BAR_KEY" as "DIV_MRKT_KEY",
    --"DaA_%_-6_MONTHS",
    --"FILL_RATE_%_-6_MONTHS",
    --"GTN_OTHER_%_-6_MONTHS",
    "RSA_%_-6_MONTHS"
    --"TOTAL_GTN_%_-6_MONTHS"
    from pnl_join)
SELECT *
FROM final