# + {"tags": ["parameters"]}
overallocation_buffer = 0.4
strategy_exclusions = ['CMBS', 'RVMBS', 'UIRS', 'UCBOND', 'FTREND', 'FIVOL', 'UXENER'] 
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

# Load up data
with pm_cache_enable():
    positions = monitoring.get_all_market_positions()
    max_sim_signal = monitoring.get_max_signal_sim()
    fund_mults_and_constraints = monitoring.get_fund_mults_and_constraints()


temp_posbounds = atd.get_posbounds_all()
multi_contracts = atd.get_multi_contracts()
mkts = positions.columns.get_level_values('market').unique().tolist()
softlimits = slim_functions.get_market_softlimits(mkts)  # atd.get_softlimits_all()
nettable_strategy_markets = posbound_functions.get_nettable_strategy_markets()

# Compute market-level posbounds
desired_posbounds = max_sim_signal * fund_mults_and_constraints
strat_mkt_positions = positions.sum(level=['strategy', 'market'], axis=1, min_count=1)
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

# Compute some derived fields
overallocation = mkt_desired_posbound / mkt_slim.replace(0, np.nan)
market_description = positions_functions.get_position_group_label(mkt_desired_posbound.index, 'client_reporting_label')
sectors = positions_functions.get_position_group_label(mkt_desired_posbound.index, 'sector')
num_strats_traded_in = positions_functions.get_num_strats_traded_in(positions, multi_contracts)

# exclude any markets that are exclusively traded by our list of strategies to exclude
# e.g. we exclude PBGA which is only traded by UXENER but we include NGL which is traded by 
# UXENER, FCOM and others, and we include positions from UXENER in the overallocation calculation
incl_mkts = list(positions.loc(axis=1)[~positions.columns.get_level_values('strategy').isin(strategy_exclusions)].columns.get_level_values('market').unique())
incl_mkts = list(set([multi_contracts.get(x, x) for x in incl_mkts]))

# Create result structure
res = pd.concat([mkt_desired_posbound, mkt_slim, overallocation,
                 market_description, num_strats_traded_in],
                axis=1,
                keys=['desired_posbound', 'slim', 'overallocation',
                      'market_description', 'num_strats_traded_in'])
res['link'] = res.index.map(lambda market: '<a href="../market_posbounds/latest?mkt={}" '
                                           'target="_blank">market notebook</a>'.format(market))
primary_cols = ['market_description', 'num_strats_traded_in', 'link']
res = res.reindex(columns=primary_cols + res.columns.drop(primary_cols).tolist())

res_incl = res.loc[incl_mkts]

#### Overallocated markets per sector

fig, ax = plt.subplots()
plotting_functions.plot_sector_overallocation(res['overallocation'], overallocation_buffer,
                                              sectors, ax, '% overallocated mkts')
plt.show()

formatter = {
    'desired_posbound': '{:,.0f}',
    'desired_posbound_with_temp': '{:,.0f}',
    'sum_atd_posbounds': '{:,.0f}',
    'slim': '{:,.0f}',
    'overallocation': '{:,.1f}x',
    'overallocation_with_temp': '{:,.1f}x',
    'num_strats_traded_in': '{:,.0f}'
}

#### Most overallocated markets

res_incl[res_incl['overallocation'] > 1 + overallocation_buffer].sort_values('overallocation', ascending=False).rename(columns={'net': ''}).style.format(formatter)
