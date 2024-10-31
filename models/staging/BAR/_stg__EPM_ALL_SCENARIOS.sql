with source as (
      select DATE_FROM_PARTS(RIGHT(YEAR,2)::INT+2000, TO_CHAR(TO_DATE(PERIOD, 'MON'), 'MM')::INT , 1)  AS "DATE_AS_OF",
      ACCOUNT,
      CUSTOMER,
      PRODUCT,
      SHIP_TOGEOGRAPHY,
      PRODUCT_DESCRIPTION,
      PRODUCT_LEVEL4,
      PRODUCT_LEVEL5,
      PRODUCT_LEVEL6,
      SUM(AMOUNT) AS AMOUNT      
      from {{ source('TEST_MARTS', 'VW_EPM_ALL_SCENARIOS') }}
      where currency = 'USD' AND SCENARIO = 'OEP_Actual' AND "VERSION" = 'PostAlloc'
      GROUP BY ALL
),
mapping as (
      SELECT *
      FROM {{ ref('TEST_account_mapping') }}), 
source_mapping as (
      SELECT scr.DATE_AS_OF,
      scr.CUSTOMER,
      scr.PRODUCT,
      scr.AMOUNT ,
      scr.SHIP_TOGEOGRAPHY,
      scr.PRODUCT_DESCRIPTION,
      scr.PRODUCT_LEVEL4,
      scr.PRODUCT_LEVEL5,
      scr.PRODUCT_LEVEL6,
      COALESCE(UPPER(map."Group"), 'NO MAPPING') as "Group"
      FROM source as scr
      LEFT JOIN mapping as map ON scr.ACCOUNT = map."Account"
) , pivot as (
      select 
      DATE_AS_OF,
      CUSTOMER,
      PRODUCT,
      SHIP_TOGEOGRAPHY,
      PRODUCT_DESCRIPTION,
      PRODUCT_LEVEL4,
      PRODUCT_LEVEL5,
      PRODUCT_LEVEL6,
      COALESCE(ROUND("'DISCOUNTS AND ALLOWANCES'",2),0) AS "DISCOUNTS AND ALLOWANCES",
      COALESCE(ROUND("'FILL RATE FINES'",2),0) AS "FILL RATE FINES",
      COALESCE(ROUND("'FREIGHT'",2),0) AS "FREIGHT",
      COALESCE(ROUND("'GROSS SALES'",2),0) AS "GROSS SALES",
      COALESCE(ROUND("'GTN OTHER'",2),0) AS "GTN OTHER",
      COALESCE(ROUND("'NO MAPPING'",2),0) AS "NO MAPPING",
      COALESCE(ROUND("'OCOS'",2),0) AS "OCOS",
      COALESCE(ROUND("'REBATES'",2),0) AS "REBATES",
      COALESCE(ROUND("'RETURNS'",2),0) AS "RETURNS",
      COALESCE(ROUND("'ROYALTIES'",2),0) AS "ROYALTIES",
      COALESCE(ROUND("'RSA'",2),0) AS "RSA",
      COALESCE(ROUND("'SG&A'",2),0) AS "SG&A",
      COALESCE(ROUND("'STD. MTL. COST'",2),0) AS "STD. MTL. COST"
      from source_mapping
      pivot(sum(AMOUNT) for "Group" in (any order by  "Group"))
      GROUP BY ALL),
TOTALS AS (
      SELECT
      DATE_AS_OF, --git test 
      CUSTOMER,
      PRODUCT,
      SHIP_TOGEOGRAPHY,
      PRODUCT_DESCRIPTION,
      PRODUCT_LEVEL4,
      PRODUCT_LEVEL5,
      PRODUCT_LEVEL6,
      "GROSS SALES",
      "RETURNS",
      "RSA",
      "ROYALTIES",
      "REBATES",
      "DISCOUNTS AND ALLOWANCES",
      "FILL RATE FINES",
      "GTN OTHER",
      "FILL RATE FINES"+"DISCOUNTS AND ALLOWANCES"+"GTN OTHER" AS "TOTAL GTN",
      "GROSS SALES" - "RSA" - "RETURNS" + "ROYALTIES" - "TOTAL GTN" AS "TOTAL NSV",
      "STD. MTL. COST",
      "FREIGHT",
      "FREIGHT" + "STD. MTL. COST" AS "TOTAL COST",
      "TOTAL NSV" - "TOTAL COST" AS "SGM",
      "OCOS",
      "SG&A",
      "NO MAPPING"
      FROM pivot
),
FINAL AS (
      SELECT *
      FROM TOTALS
      WHERE ("DISCOUNTS AND ALLOWANCES"+"FILL RATE FINES"+"FREIGHT"+"GROSS SALES"+"GTN OTHER"+"NO MAPPING"+ "OCOS"+"REBATES"+"RETURNS"+"ROYALTIES"+"RSA"+"SG&A"+"STD. MTL. COST") <> 0 )
SELECT *
FROM FINAL

