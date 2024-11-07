select *
from {{ source('DEV_AIDA', 'FactDEMAND_FORECAST') }}