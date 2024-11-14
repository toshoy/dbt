with source as (
      select *,
        DATE_FROM_PARTS(FISCAL_YEAR,FISCAL_MONTH, 1) AS "DATE_AS_OF" ,
        round(cos_st_cost,2) AS "COS_ST_COST_FIX"
        from {{ ref('_stg__VW_PROFIT') }}
        where round(cos_st_cost,2) <> 0 and "NET_SHIP_QTY" <> 0),
date_filter as (
    select *,
    (select max("DATE_AS_OF") from source ) as "MAX_DATE",
    (select DATEADD(MONTH,{{var('rolling_avg')}},MAX("DATE_AS_OF")) AS MIN_DATE FROM source) as "MIN_DATE"
    from source
    WHERE "DATE_AS_OF" >= (select DATEADD(MONTH,{{var('rolling_avg')}},MAX("DATE_AS_OF")) AS MIN_DATE FROM source)
),
trans as (
    SELECT 
    product_material_code,
    MIN("DATE_AS_OF") AS "MIN_DATE_AS_OF",
    MAX("DATE_AS_OF") AS "MAX_DATE_AS_OF",
    sum(net_ship_qty) as "NET_SHIP_QTY",
    ROUND(sum(CO_OB_FREIGHT),2) AS "FREIGHT",
    ROUND(sum(NET_SHIP_GSV_W_O_RSA),2) AS "GSV_W_O_RSA",
    round(sum("COS_ST_COST_FIX"),2) as "COS_ST_COST"
    FROM date_filter
    GROUP BY ALL),
final as (
    SELECT
    product_material_code,
    "MIN_DATE_AS_OF",
    "MAX_DATE_AS_OF",
    "NET_SHIP_QTY",
    "COS_ST_COST",
    "GSV_W_O_RSA",
    IFF(div0("COS_ST_COST","NET_SHIP_QTY") < 0 , 0 ,round(div0("COS_ST_COST","NET_SHIP_QTY"),3))  as "UNIT_STD_MAT_COST", --excluding negative values
    "FREIGHT",
    CASE 
    WHEN "FREIGHT" > "GSV_W_O_RSA" THEN 0
    ELSE 
    IFF(div0("FREIGHT","GSV_W_O_RSA") < 0 , 0 ,round(div0("FREIGHT","GSV_W_O_RSA"),4)) 
    END AS "%_FREIGHT" --excluding negative values
    from trans)
SELECT *
FROM final
order by "%_FREIGHT"  desc 
{# where product_material_code = 'DW4802' #}