with shipment_date as (
    select 
    SI.*,
    DD.DATE as "SHIP_DATE"
    FROM {{ ref('_stg__shipment_invoices') }} AS  SI
    INNER JOIN {{ ref('_stg__Ddate') }} AS  DD ON SI.SHIPMENT_DATE_KEY = DD.DATE_KEY
),
INVOICE_DATE AS (
    select SD.*, 
    DD.DATE as "ORDER_DATE"
    FROM shipment_date AS SD
    INNER JOIN {{ ref('_stg__Ddate') }} DD ON SD.INVOICE_DATE_KEY = DD.DATE_KEY),
FISCAL_DATE AS (
    SELECT 
    ID.*,
    DD."Date",
    DD.F_WEEK_START_DATE,
    DD.F_YEAR
    FROM INVOICE_DATE AS ID
    INNER JOIN  {{ ref('_stg__DimCalendar') }} AS DD ON ID.SHIP_DATE = DD."Date")
SELECT *
FROM FISCAL_DATE