with source as (
      select *,
        DATE_FROM_PARTS(FISCAL_YEAR,FISCAL_MONTH, 1) AS "DATE_AS_OF" 
        from {{ ref('_stg__VW_PROFIT') }}),
        {# where CO_OB_FREIGHT < NET_SHIP_GSV_W_O_RSA), --IMPORTANT CONDITIONAL FOR EXLUDING SKU WITH GSA < FREIGHT #}
date_filter as (
    select *,
    (select max("DATE_AS_OF") from source ) as "MAX_DATE",
    (select DATEADD(MONTH,{{var('rolling_avg')}},MAX("DATE_AS_OF")) AS MIN_DATE FROM source) as "MIN_DATE"
    from source
    WHERE "DATE_AS_OF" >= (select DATEADD(MONTH,{{var('rolling_avg')}},MAX("DATE_AS_OF")) AS MIN_DATE FROM source)
)
,trans as (
    SELECT 
    product_material_code,
    MIN("DATE_AS_OF") AS "MIN_DATE_AS_OF",
    MAX("DATE_AS_OF") AS "MAX_DATE_AS_OF",
    ROUND(sum(CO_OB_FREIGHT),2) AS "FREIGHT",
    ROUND(sum(NET_SHIP_GSV_W_O_RSA),2) AS "GSV_W_O_RSA"
    FROM date_filter
    GROUP BY ALL)
, final as (
    SELECT
    product_material_code,
    "MIN_DATE_AS_OF",
    "MAX_DATE_AS_OF",
    "GSV_W_O_RSA",
    "FREIGHT"
    ,CASE 
    WHEN "FREIGHT" > "GSV_W_O_RSA" THEN 0
    ELSE 
    IFF(div0("FREIGHT","GSV_W_O_RSA") < 0 , 0 ,round(div0("FREIGHT","GSV_W_O_RSA"),4)) 
    END AS "%_FREIGHT" --excluding negative values
    from trans)
SELECT *
FROM source
where product_material_code = 'U60000008'