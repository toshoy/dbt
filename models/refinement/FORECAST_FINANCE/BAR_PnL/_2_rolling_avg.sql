with sources as (
    select *,
    (select DATEADD(MONTH,{{var('rolling_avg')}},MAX(F_MONTH_DATE)) AS MIN_DATE from {{ ref('TEST_BAR_PnL') }}) as MIN_DATE,
    (select max(F_MONTH_DATE) from {{ref('TEST_BAR_PnL')}}) AS "MAX_DATE"
    from {{ ref('TEST_BAR_PnL') }}),
filtering as (
    SELECT
    --bar_key,
    BAR_KEY_ADJ,
    sum("GROSS SALES") AS "GROSS SALES {{var('rolling_avg')}} MONTHS",
    SUM(RSA) AS "RSA {{var('rolling_avg')}} MONTHS",
    SUM("DISCOUNTS AND ALLOWANCES") AS "DaA {{var('rolling_avg')}} MONTHS",
    SUM("FILL RATE FINES") AS "FILL RATE FINES {{var('rolling_avg')}} MONTHS",
    SUM("GTN OTHER") AS "GTN OTHER {{var('rolling_avg')}} MONTHS",
    SUM("TOTAL GTN") AS "TOTAL GTN {{var('rolling_avg')}} MONTHS",
    FROM sources
    where F_MONTH_DATE >= MIN_DATE
    GROUP BY ALL),
FINAL AS (
    SELECT *,
    div0("RSA {{var('rolling_avg')}} MONTHS","GROSS SALES {{var('rolling_avg')}} MONTHS") AS "RSA_%_{{var('rolling_avg')}}_MONTHS",
    div0("DaA {{var('rolling_avg')}} MONTHS","GROSS SALES {{var('rolling_avg')}} MONTHS") AS "DaA_%_{{var('rolling_avg')}}_MONTHS",
    div0("FILL RATE FINES {{var('rolling_avg')}} MONTHS","GROSS SALES {{var('rolling_avg')}} MONTHS") AS "FILL_RATE_%_{{var('rolling_avg')}}_MONTHS",
    div0("GTN OTHER {{var('rolling_avg')}} MONTHS","GROSS SALES {{var('rolling_avg')}} MONTHS") AS "GTN_OTHER_%_{{var('rolling_avg')}}_MONTHS",
    div0("TOTAL GTN {{var('rolling_avg')}} MONTHS","GROSS SALES {{var('rolling_avg')}} MONTHS") AS "TOTAL_GTN_%_{{var('rolling_avg')}}_MONTHS"
    FROM filtering)
select *
from FINAL



