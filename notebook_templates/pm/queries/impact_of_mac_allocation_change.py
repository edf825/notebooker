# ### Impact of MAC allocation changes
# 
# Given a proposed set of new mac allocations, what is the impact on the CPM of that mac and it's risk allocations

# %matplotlib inline 

import pandas as pd
import ahl.returnbreakdown.api as arb


# + {"tags": ["parameters"]}
mac = 'MCR0'
new_allocs = {
    'MOKUM': 0.37,
    'FCORE': 0.12,
    'ECOFI': 0.07,
    'ECOFX': 0.09,
    'XCOMSPD': 0.14,
    'BOX10': 0.21
}
returns_since = str(pd.datetime.now().year - 4)

# -

current_sim_rets = arb.vol_normalised_overlapping_returns(mac=mac)

new_sim_rets = arb.get_mac_returns_new_allocations(mac, new_allocs, 'sim')
normalised_new_sim_rets = arb.normalise_using_strategy_sim_vol(arb.overlapping_returns(new_sim_rets, 5))

current_sim_strategy_rets = current_sim_rets.groupby(level='strategy', axis=1).sum()
new_sim_strategy_rets = normalised_new_sim_rets.groupby(level='strategy', axis=1).sum()

# #### Current calculated CPM

current_cpm = current_sim_strategy_rets.loc[returns_since:].std().sum() / current_sim_strategy_rets.loc[returns_since:].sum(axis=1).std()
current_cpm

# #### New calculated CPM

new_cpm = new_sim_strategy_rets.loc[returns_since:].std().sum() / new_sim_strategy_rets.loc[returns_since:].sum(axis=1).std()
new_cpm

# #### CPM Percentage Change
(new_cpm / current_cpm - 1) * 100


# #### Strategy Risk Allocation Change

pd.concat({
    'current': arb.allocations_at_level(current_sim_rets, level='strategy'),
    'new': arb.allocations_at_level(normalised_new_sim_rets, level='strategy'),
}, axis=1).plot(kind='bar', figsize=(20,5))

