with MTD_EXCEL_UFR_DBT as (
      select * from {{ ref('TEST_FactMTD_EXCEL_UFR_DBT') }}
)
select 
*
from MTD_EXCEL_UFR_DBT