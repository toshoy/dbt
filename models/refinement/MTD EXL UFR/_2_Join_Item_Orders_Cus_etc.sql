with item as (
    SELECT 
    TD.*,
    DI.ITEM_NUMBER,
    DI.ITEM_DESCRIPTION,
    DI.ITEM_TYPE_DESCRIPTION
    FROM {{ ref('_1_Transform_dates') }} AS TD
    INNER JOIN {{ ref('_stg__dItems') }} AS DI ON TD.ITEM_KEY = DI.ITEM_KEY),
Order_div as (
    SELECT TD.*,
    DO.DIVISION,
    DO.QUOTE_ORDER_NUMBER
    FROM item AS TD
    INNER JOIN {{ ref('_stg__DOrders') }} AS DO ON TD.ORDER_KEY = DO.ORDER_KEY),
Customer as (
    SELECT TD.*,
    DC.customer_number,
    DC."CUSTOMER_NAME" ,
    DC.entity_group,
    DC.entity_group_name
    FROM Order_div AS TD
    INNER JOIN {{ ref('_stg__DCustomers') }} AS DC ON TD.customer_key = DC.customer_key),
dmd as (
    SELECT TD.*,
    DCD.demand_group
    FROM Customer AS TD
    INNER JOIN {{ ref('_stg__DCustDiv') }} AS DCD ON TD.customer_key = DCD.customer_key AND TD.division = DCD.division),
warehouse as (
    SELECT
    TD.*,
    DW.CORP_WAREHOUSE_CODE
    FROM dmd AS TD
    INNER JOIN {{ ref('_stg__dWarehouse') }} AS DW ON TD.SHIP_FROM_WAREHOUSE_KEY = DW.WAREHOUSE_KEY),
mrkt as (
    SELECT TD.*,
    MCK.mrkt_chnl_key
    FROM warehouse AS TD
    LEFT OUTER JOIN {{ ref('_stg__Mrky_Chnl_key_lookup') }} AS MCK ON TD.CUSTOMER_NUMBER = MCK.CUSTOMER_NUMBER AND TD.DIVISION = MCK.DIVISION)
SELECT 
SOURCE,
F_WEEK_START_DATE,
ITEM_NUMBER AS "SKU_KEY",
ITEM_DESCRIPTION AS "SKU_DESC",
DIVISION,
CORP_WAREHOUSE_CODE AS "LOC_ID",
MRKT_CHNL_KEY,
SUM(SHIPPED_UNITS) AS "UFR_QTY",
SUM(SHIPPED_DOLLARS) AS "UFR_GSV",
SUM(ORDERED_UNITS) AS "UFR_NET_ORDER_QTY",
SUM(ORDERED_DOLLARS) AS "UFR_NET_ORDER_GSV"
FROM mrkt
GROUP BY ALL 













