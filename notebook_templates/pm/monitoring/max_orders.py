# + {"tags": ["parameters"]}
strategy_exclusions = ['BOX10','CETF','CIRATE','CMBS','CSWARM','DCREDIT','FETF','FIVOL','FSETT','FTREND','RVMBS','SWX10','UCBOND','UCREDIT','UIRS','UXCURR','UXENER']
strategy_inclusions = None # overrides exclusions
include_insight_strats = False
num_results = 50
# -

# %matplotlib inline
# imports
import ahl.tradingdata as atd
from datetime import datetime as dt
import pandas as pd
import numpy as np
import ahl.marketdata as amd
import pm.data.strategies as pds
from pm.monitoring.caching import pm_cache_enable
import pm.monitoring.positions as pmp
import risk.data.liquidity as rdl
from collections import OrderedDict

with pm_cache_enable():
    positions = pmp.get_all_market_positions()

# get positions and exclude manually excluded and insight only strats
# this is only to establish what markets we look at
positions = positions.loc(axis=1)[positions.abs().sum() > 0]
if strategy_inclusions is None:
    positions_incl = positions.loc(axis=1)[~positions.columns.get_level_values(0).isin(strategy_exclusions)]
    positions_incl = positions_incl.loc(axis=1)[positions_incl.columns.get_level_values(0).isin(pds.get_strategies(include_insight_only=include_insight_strats))]
else:
    positions_incl = positions.loc(axis=1)[strategy_inclusions]

strat_mkts = positions_incl.head().columns.droplevel(2).tolist()

all_sample_times = atd.get_sample_times()
sample_times = [all_sample_times[s][m] for s,m in strat_mkts]

sum_max_orders = [sum([x['max_order'] for x in y.values() if x['max_order'] is not None]) for y in sample_times]
max_order_types = [set([x['max_order_type'] for x in y.values()]) for y in sample_times]
num_samples = [len(y) for y in sample_times]

mkts = set(x for y,x in strat_mkts)
risk_volumes = rdl.get_volume_data(dt.now(),mkts).reindex(mkts)
mkt_median_volumes = risk_volumes['median_volume'].to_dict()
median_volumes = [mkt_median_volumes[m] for (s,m) in strat_mkts]

max_order_as_pct_of_volume = [a / b for (a,b) in zip(sum_max_orders, median_volumes)]

# ### Distribution of max_order_as_pct_of_volume

pd.Series(max_order_as_pct_of_volume).hist()

res = pd.DataFrame(index=strat_mkts,data=OrderedDict([('sum_max_orders',sum_max_orders),
                                                        ('max_order_types',max_order_types),
                                                        ('num_samples',num_samples),
                                                        ('median_mkt_volume',median_volumes),
                                                        ('max_order_as_pct_of_volume',max_order_as_pct_of_volume)]))
res.index = pd.MultiIndex.from_tuples(res.index).reorder_levels([1,0])

# ### Largest ratios of max orders to market volume
res.nlargest(num_results,'max_order_as_pct_of_volume').reset_index()