select 
*,
'{{invocation_id}}' AS "BATCH_ID",
CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', CURRENT_TIMESTAMP()) AS "RUN_UPD_DTE"
from {{ ref('_1_mapping_pivot') }}