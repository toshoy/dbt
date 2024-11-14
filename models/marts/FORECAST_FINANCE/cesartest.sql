select region , sum("GROSS SALES")
from dev_aida.rgm_price_tracker.TEST_BAR_PnL
group by all
limit 100