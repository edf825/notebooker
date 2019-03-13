# ---
# jupyter:
#   celltoolbar: Tags
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: pm_monitoring
#     language: python
#     name: pm_monitoring
# ---

# + {"tags": ["parameters"]}
overallocation_buffer = 0.4
# -

# imports
# %matplotlib inline
import matplotlib.pyplot as plt
import pm.monitoring as monitoring
from pm.monitoring.caching import pm_cache_enable
import pm.monitoring.positions as positions_functions
import pm.monitoring.plotting as plotting_functions
import pm.monitoring.posbound as posbound_functions
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
softlimits = atd.get_softlimits_all()

# Compute market-level posbounds

desired_posbounds = max_sim_signal * fund_mults_and_constraints
strat_mkt_positions = positions.sum(level=['strategy', 'market'], axis=1, min_count=1)
scaled_positions = strat_mkt_positions * fund_mults_and_constraints
desired_posbounds_with_temp = posbound_functions.apply_temp_posbounds(desired_posbounds, temp_posbounds)
mkt_desired_posbound = posbound_functions.get_market_posbounds(desired_posbounds, multi_contracts)
mkt_desired_posbound_with_temp_long = posbound_functions.get_market_posbounds(
    desired_posbounds_with_temp.xs('long', level=2, axis=1), multi_contracts)
mkt_desired_posbound_with_temp_short = posbound_functions.get_market_posbounds(
    desired_posbounds_with_temp.xs('short', level=2, axis=1), multi_contracts)
mkt_desired_posbound_with_temp = pd.concat([mkt_desired_posbound_with_temp_long,
                                            -1.0 * mkt_desired_posbound_with_temp_short], axis=1).max(axis=1)
mkt_slim = pd.Series(softlimits).reindex_like(mkt_desired_posbound)

# Compute some derived fields

overallocation = mkt_desired_posbound / mkt_slim.replace(0, np.nan)
overallocation_with_temp = mkt_desired_posbound_with_temp / mkt_slim.replace(0, np.nan)
market_description = positions_functions.get_position_group_label(mkt_desired_posbound.index, 'client_reporting_label')
sectors = positions_functions.get_position_group_label(mkt_desired_posbound.index, 'sector')
num_strats_traded_in = positions_functions.get_num_strats_traded_in(positions, multi_contracts)

# Create result structure

res = pd.concat([mkt_desired_posbound, mkt_desired_posbound_with_temp, mkt_slim, overallocation,
                 overallocation_with_temp, market_description, num_strats_traded_in],
                axis=1,
                keys=['desired_posbound', 'desired_posbound_with_temp', 'slim', 'overallocation',
                      'overallocation_with_temp', 'market_description', 'num_strats_traded_in'])
# TODO: use new searching facility
res['link'] = res.index.map(lambda x: '<a href="mkt_{}" target="_blank">market notebook</a>'.format(x))
primary_cols = ['market_description', 'num_strats_traded_in', 'link']
res = res.reindex(columns=primary_cols + res.columns.drop(primary_cols).tolist())

#### Overallocated markets per sector

fig, ax = plt.subplots(1, 2, figsize=(12, 6), sharey=True)
plotting_functions.plot_sector_overallocation(res['overallocation'], overallocation_buffer,
                                              sectors, ax[0], '% overallocated mkts')
plotting_functions.plot_sector_overallocation(res['overallocation_with_temp'], overallocation_buffer,
                                              sectors, ax[1], '% overallocated mkts (temp posbounds applied)')
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

res.nlargest(10, 'overallocation_with_temp').rename(columns={'net': ''}).style.format(formatter)

#### Most overallocated markets traded in multiple strats

res[res['num_strats_traded_in'] > 1].nlargest(10, 'overallocation_with_temp').rename(columns={'net': ''}).style.format(formatter)
