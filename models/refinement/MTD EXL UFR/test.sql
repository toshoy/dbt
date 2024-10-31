{% set old_query %}
    select * from DEV_AIDA.RGM_PRICE_TRACKER."TEST_FactMTD_EXCEL_UFR"
{% endset %}
 
{% set new_query %}
    select * from {{ ref('TEST_FactMTD_EXCEL_UFR_DBT') }}
{% endset %}

{{ audit_helper.snowflake__quick_are_queries_identical(
    query_a = old_query,
    query_b = new_query,
    columns=['SKU_KEY', 'SOURCE']
  ) 
}}