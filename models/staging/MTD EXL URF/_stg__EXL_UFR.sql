with exl_ufr_max as (
            select 
            YEAR,
            WEEK,
            MAX(LOADDTS) AS "Max_LOADDTS"
            from {{ source('PROD_RAW', 'VW_NOCUFROTIF_HISTORY') }}
            GROUP BY 1,2 
),
exl_ufr_all as (
            select * , 'PROD_RAW.EXCEL' as "SOURCE"
            from {{ source('PROD_RAW', 'VW_NOCUFROTIF_HISTORY') }}

),

exl_ufr_union as (
        Select exuf.*
        from exl_ufr_all exuf
        inner join exl_ufr_max exall on exuf.YEAR = exall.YEAR and exuf.WEEK = exall.WEEK and exuf.LOADDTS = exall."Max_LOADDTS"
)
select
EXL.SOURCE,
DIMCAL.F_WEEK_START_DATE,
EXL.SKU_ID AS "SKU_KEY",
EXL.SKU_DESC,
EXL.DEMAND_GROUP_ID AS "MRKT_CHNL_KEY",
EXL.ERP_LOCATION_ID_SITE AS "LOC_ID",
SUM(EXL.UFRNETORDERQTY) AS "UFR_NET_ORDER_QTY",
SUM(EXL.UFRQTY) AS "UFR_QTY",
SUM(EXL.UFRNETORDERGSV) AS "UFR_NET_ORDER_GSV",
SUM(EXL.UFRGSV) AS "UFR_GSV",
SUM(EXL.OTIFNETORDERQTY) AS "OTIF_NET_ORDER_QTY",
SUM(EXL.OTIFQTY) AS "OTIF_QTY",
SUM(EXL.OTIFNETORDERGSV) AS "OTIF_NET_ORDER_GSV",
SUM(EXL.OTIFGSV) AS "OTIF_GSV",
from exl_ufr_union EXL
JOIN {{ ref('_stg__DimCalendar') }} DIMCAL ON EXL.YEAR = DIMCAL.C_YEAR AND EXL.WEEK = DIMCAL.C_WEEK_NBR
GROUP BY ALL