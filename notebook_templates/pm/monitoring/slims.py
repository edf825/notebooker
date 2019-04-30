# + {"tags": ["parameters"]}
strategy_exclusions = ['BOX10','CETF','CIRATE','CMBS','CSWARM','DCREDIT','FETF','FIVOL','FSETT','FTREND','RVMBS','SWX10','UCBOND','UCREDIT','UIRS','UXCURR','UXENER']
strategy_inclusions = []
include_insight_strats = False
lookback = 3*260
cutoff = 0.
# -

# imports
# %matplotlib inline
import matplotlib.pyplot as plt
import pm.monitoring as monitoring
from pm.monitoring.caching import pm_cache_enable
import pm.monitoring.slim as slim_functions
import ahl.tradingdata as atd
import numpy as np
import pandas as pd
from collections import OrderedDict
import pm.data.strategies as pds
from risk.data.liquidity import get_volume_data
from datetime import datetime as dt
from ahl.db import DOTS_DB, ARM_DB
from IPython.display import HTML

# Load up data
with pm_cache_enable():
    positions = monitoring.get_all_market_positions()

# establish our list of markets and then filter to that
def get_list_mkts_from_positions(positions,excl_strats=[],incl_strats=[],include_insight=False):
    strat_mkts = positions.groupby(level=['strategy','market'],axis=1).sum().abs().sum().replace(0,np.nan).dropna().index.tolist()
    if incl_strats == []:
        if excl_strats is not []:
            incl_strats = set(pds.get_strategies(include_insight_only=include_insight)) - set(excl_strats)
        else:
            incl_strats = list(set([m for (s,m) in strat_mkts]))
    return sorted(list(set([m for (s, m) in strat_mkts if s in incl_strats])))

def get_contract_limits():
    return DOTS_DB.db_query('select instrument_id, spot_limit, contract_limit, total_limit FROM dots.future',name='pd')

def get_mkt_info():
    return ARM_DB.db_query("select * from tsr.instrument_map order by 1", name='pd')

# decide on markets
mkts = get_list_mkts_from_positions(positions,excl_strats=strategy_exclusions,incl_strats=strategy_inclusions,include_insight=include_insight_strats)
multi_contracts = atd.get_multi_contracts()
mkt_families = sorted(list(set(multi_contracts.get(x,x) for x in mkts)))

# get data
softlimits = atd.get_softlimits_all(mkt_families)
real_softlimits = slim_functions.get_market_softlimits(mkt_families)
vol_carveouts = {m:s - real_softlimits[m] for m,s in softlimits.items()}
risk_volume_data = get_volume_data(dt.now(),mkt_families)
limits = get_contract_limits().reindex(mkt_families)
softlimit_vs_median_volume = pd.Series(softlimits) / risk_volume_data['median_volume']
mkt_info = get_mkt_info()

min_volume_and_limits = pd.concat([risk_volume_data['median_volume'],limits[['contract_limit','total_limit']]],axis=1).min(axis=1)
softlimit_vs_min_volume_and_limits = pd.Series(softlimits) / min_volume_and_limits

# decide on sensible grouping
grouping = mkt_info['sector_id'].reindex(mkt_families)

#### Softlimits, exchange limits and volumes

res = pd.DataFrame(
    index=mkt_families,
    data=OrderedDict([
            ('softlimit',softlimits),
            ('vol_carveout',vol_carveouts),
            ('softlimit_post_carveout',real_softlimits),
            ('spot_limit',limits['spot_limit']),
            ('contract_limit',limits['contract_limit']),
            ('total_limit',limits['total_limit']),
            ('median_volume',risk_volume_data['median_volume']),
            ('softlimit_vs_volume_or_limit',softlimit_vs_min_volume_and_limits)
                    ]))

res.index = pd.MultiIndex.from_tuples(zip(grouping,mkt_families))
res = res.sort_index()

# pretty results
html_res = ''
for grp in set(grouping):
    html_res += '<b>{}</b>\n\n'.format(grp)
    html_res += res.loc[grp].sort_values('softlimit_vs_volume_or_limit',ascending=False).style.format('{:,.0f}').format('{:,.0%}',subset='softlimit_vs_volume_or_limit').render().replace('nan%','-').replace('nan','-')
    html_res += '\n\n'
HTML(html_res)