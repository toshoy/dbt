with mtd as (
    select 
        source,
        F_WEEK_START_DATE,
        SKU_KEY,
        SKU_DESC,
        UFR_QTY,
        UFR_GSV,
        UFR_NET_ORDER_QTY,
        UFR_NET_ORDER_GSV,
        NULL AS OTIF_NET_ORDER_QTY,
        NULL AS OTIF_QTY,
        NULL AS OTIF_NET_ORDER_GSV,
        NULL AS OTIF_GSV,
        DIVISION,
        LOC_ID,
        MRKT_CHNL_KEY
    from {{ ref('_2_Join_Item_Orders_Cus_etc') }}
),
exl as (
    select 
        source,
        F_WEEK_START_DATE,
        SKU_KEY,
        SKU_DESC,
        UFR_QTY,
        UFR_GSV,
        UFR_NET_ORDER_QTY,
        UFR_NET_ORDER_GSV,
        OTIF_NET_ORDER_QTY,
        OTIF_QTY,
        OTIF_NET_ORDER_GSV,
        OTIF_GSV,
        NULL AS DIVISION,
        LOC_ID,
        MRKT_CHNL_KEY
    from {{ ref('_stg__EXL_UFR') }}
),
otd_urf as (
    select *
    from mtd
    union all 
    select *
    from exl
)
select 
        source,
        DATEADD(DAY,3,"F_WEEK_START_DATE") AS "DATE_AS_OF",
        UPPER(COALESCE(TRIM("SKU_KEY"),'')) AS SKU_KEY,
        SKU_DESC,
        UFR_QTY,
        UFR_GSV,
        UFR_NET_ORDER_QTY,
        UFR_NET_ORDER_GSV,
        OTIF_NET_ORDER_QTY,
        OTIF_QTY,
        OTIF_NET_ORDER_GSV,
        OTIF_GSV,
        IFF(
            "DIVISION" <> '',
            "DIVISION",
            'N/A'
        ) AS DIVISION,
        LOC_ID,
        IFF(
            "MRKT_CHNL_KEY" <> '',
            "MRKT_CHNL_KEY",
            'N/A'
        ) AS MRKT_CHNL_KEY,
        HASH(sku_key,date_as_of, division, loc_id, mrkt_chnl_key)::VARCHAR AS "RCRD_HASH_KEY"
from otd_urf