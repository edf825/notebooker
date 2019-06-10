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
import pm.monitoring.positions as positions_functions
import pm.monitoring.plotting as plotting_functions
import pm.monitoring.posbound as posbound_functions
import pm.monitoring.slim as slim_functions
import ahl.tradingdata as atd
import numpy as np
import pandas as pd
from collections import OrderedDict
import pm.data.strategies as pds

# Load up data
with pm_cache_enable():
    positions = monitoring.get_all_market_positions()
    max_sim_signal = monitoring.get_max_signal_sim()
    fund_mults_and_constraints = monitoring.get_fund_mults_and_constraints()

temp_posbounds = atd.get_posbounds_all()
multi_contracts = atd.get_multi_contracts()
nettable_strategy_markets = posbound_functions.get_nettable_strategy_markets()

# establish our list of markets and then filter to that
def get_list_mkts_from_positions(positions,excl_strats=[],incl_strats=[],include_insight=False):
    strat_mkts = positions.groupby(level=['strategy','market'],axis=1).sum().abs().sum().replace(0,np.nan).dropna().index.tolist()
    if incl_strats == []:
        if excl_strats is not []:
            incl_strats = set(pds.get_strategies(include_insight_only=include_insight)) - set(excl_strats)
        else:
            incl_strats = list(set([m for (s,m) in strat_mkts]))
    return sorted(list(set([m for (s, m) in strat_mkts if s in incl_strats])))

mkts = get_list_mkts_from_positions(positions,excl_strats=strategy_exclusions,incl_strats=strategy_inclusions,include_insight=include_insight_strats)
mkt_families = sorted(list(set(multi_contracts.get(x,x) for x in mkts)))

softlimits = slim_functions.get_market_softlimits(mkt_families)

# sort out our data - note here we are only excluding markets exclusively traded by excluded strategies
strat_mkt_positions = positions.groupby(level=['strategy','market'],axis=1).sum().loc(axis=1)[:,mkts]
max_sim_signal = max_sim_signal.reindex_like(strat_mkt_positions)
fund_mults_and_constraints = fund_mults_and_constraints.reindex(strat_mkt_positions.columns)

# Compute market-level posbounds
desired_posbounds = max_sim_signal * fund_mults_and_constraints
scaled_positions = strat_mkt_positions * fund_mults_and_constraints
desired_posbounds_with_temp = posbound_functions.apply_temp_posbounds(desired_posbounds, temp_posbounds)
desired_posbounds_with_temp_and_net = posbound_functions.remove_nettable_strategy_market_posbounds(desired_posbounds_with_temp,
                                                                                                   nettable_strategy_markets)
# only care about with temp and net now
mkt_desired_posbound_long = posbound_functions.get_market_posbounds(
    desired_posbounds_with_temp_and_net.xs('long', level=2, axis=1), multi_contracts)

mkt_desired_posbound_short = posbound_functions.get_market_posbounds(
    desired_posbounds_with_temp_and_net.xs('short', level=2, axis=1), multi_contracts)

mkt_desired_posbound = pd.concat([mkt_desired_posbound_long,
                                  -1.0 * mkt_desired_posbound_short], axis=1).max(axis=1)

mkt_slim = pd.Series(softlimits).reindex_like(mkt_desired_posbound)

# position stuff
def apply_temp_posbounds_to_positions(scaled_positions,temp_posbounds):
    # scaled positions columns must be strategy, market
    res = scaled_positions.copy()
    for (s,m) in res.columns:
        temp_all = temp_posbounds.get(s,{}).get(m,{})
        temp_long = temp_all.get('temp_long',None)
        temp_short = temp_all.get('temp_short',None)
        res.loc(axis=1)[(s,m)] = res.loc(axis=1)[(s,m)].clip(upper=temp_long,lower=temp_short)
    return res

scaled_positions_with_temp = apply_temp_posbounds_to_positions(scaled_positions,temp_posbounds)
scaled_positions_with_temp_and_net = posbound_functions.remove_nettable_strategy_market_posbounds(scaled_positions_with_temp,nettable_strategy_markets)

mkt_desired_position_long = scaled_positions_with_temp_and_net.clip(lower=0).groupby(level='market',axis=1).sum().groupby(lambda x: multi_contracts.get(x,x),axis=1).sum()
mkt_desired_position_short = scaled_positions_with_temp_and_net.clip(upper=0).groupby(level='market',axis=1).sum().groupby(lambda x: multi_contracts.get(x,x),axis=1).sum()

gross_longs_over_slim = mkt_desired_position_long.subtract(mkt_slim,axis=1).clip(lower=0)
gross_shorts_over_slim = mkt_desired_position_short.abs().subtract(mkt_slim,axis=1).clip(lower=0)
sum_gross_backups = gross_longs_over_slim + gross_shorts_over_slim

num_backups = (sum_gross_backups>0).tail(lookback).sum()
pct_of_time_spent_backed_up = 1. * num_backups / lookback

avg_backup = sum_gross_backups.tail(lookback).replace(0,np.nan).mean().fillna(0)
avg_backup_pct = avg_backup / (mkt_slim + avg_backup)

# Compute some derived fields
sum_posbounds_over_slim = mkt_desired_posbound / mkt_slim.replace(0, np.nan)
market_description = positions_functions.get_position_group_label(mkt_desired_posbound.index, 'client_reporting_label')
sectors = positions_functions.get_position_group_label(mkt_desired_posbound.index, 'sector')
num_strats_traded_in = positions_functions.get_num_strats_traded_in(positions, multi_contracts)
links = {m:'<a href="../market_posbounds/latest-successful-asof?mkt={}" '
                    'target="_blank">market notebook</a>'.format(m) for m in mkt_families}


#### Overallocated markets per sector

overallocated = pct_of_time_spent_backed_up > cutoff
fig, ax = plt.subplots()
overallocated.groupby(sectors).apply(lambda x:1.*x.sum() / len(x)).to_frame().plot(kind='bar',ax=ax)
plt.legend(['% overallocated markets'])

#### Most overallocated markets

res = pd.DataFrame(index=mkt_families,data=OrderedDict([
    ('market_description',market_description),
    ('num_strats',num_strats_traded_in),
    ('link',links),
    ('backup_frequency',pct_of_time_spent_backed_up),
    ('average_backup',avg_backup_pct),
    ('sum_posbounds_over_slim', sum_posbounds_over_slim)
]))
formatter = {
    'sum_posbounds_over_slim': '{:,.0%}',
    'backup_frequency': '{:,.0%}',
    'average_backup': '{:,.0%}'
}
res[res['backup_frequency'] > cutoff].sort_values('backup_frequency', ascending=False).style.format(formatter)
