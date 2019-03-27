# ---
# jupyter:
#   celltoolbar: Tags
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: pm_notebook_kernel
#     language: python
#     name: pm_notebook_kernel
# ---

# + {"tags": ["parameters"]}
mkt = 'SGN'
# -

# imports
# %matplotlib inline
import matplotlib.pyplot as plt
import pm.monitoring as monitoring
from pm.monitoring.caching import pm_cache_enable
import ahl.tradingdata as atd
import pandas as pd
import numpy as np
from ahl.logging import logger
from ahl.positionmanager.posbounds_calculation_service import _get_slim_for
from ahl.positionmanager.api import get_dataservice
from ahl.positionmanager.repositories import StaticDataRepository
import pm.monitoring.posbound as posbound_functions
import ahl.pandas as apd
from ahl.db import DOTS_DB

TIMESERIES_CHART_LOOKBACK = 5 * 260
POS_DIST_CHART_LOOKBACK = 3 * 260

# Load up initial data
with pm_cache_enable():
    positions = monitoring.get_all_market_positions()
    max_sim_signal = monitoring.get_max_signal_sim()
    fund_mults_and_constraints = monitoring.get_fund_mults_and_constraints()

# Compute market-level posbounds
desired_posbounds = max_sim_signal * fund_mults_and_constraints
strat_mkt_positions = positions.sum(level=['strategy', 'market'], axis=1, min_count=1)
scaled_positions = strat_mkt_positions * fund_mults_and_constraints


# this is also used in the overall posbound notebook - should pull it out to pm.monitoring functions 
def get_net_in_posman_all():
    ''' FIXME: I am ignoring the system level because I haven't built my position dataframes around systems
    Theoretically if we had the same market in multiple systems in one strat, and one was net_in_posman'd
    but the other wasn't, we would have an issue. As it is it's probs fine.
    '''
    d = DOTS_DB.db_query('select * from dots.system where net_in_posman=1', name='pd')
    res = list(d['system_id'].items())
    sys_map = atd.get_system_instrument_map()
    res1 = []
    for x in res:
        mkts = sys_map[x[0]][x[1]]
        for m in mkts:
            res1.append((x[0], m))
    return res1


# this is also used in the overall posbound notebook - should pull it out to pm.monitoring functions 
def apply_net_in_posmans(desired_posbounds, net_in_posmans):
    res = desired_posbounds.copy()
    for col in res.columns:
        strat = col[0]
        mkt = col[1]
        if (strat, mkt) in net_in_posmans:
            res[col] = desired_posbounds[col].clip(upper=0, lower=0)
    return res


# add multi contract levels 
def add_multi_contract_market_level(pos_df, multi_contract_mapping):
    mkt_level = pos_df.columns.get_level_values('market')
    contract_mapping = atd.get_multi_contracts()
    comb_mkt_level = [multi_contract_mapping.get(x, x) for x in mkt_level]
    res = apd.add_level_to_index(pos_df, 'multi_contract_market', comb_mkt_level, axis=1)
    # reorder levels, generalised to allow for any number of levels above market
    new_level_order = [x for x in pos_df.columns.names if x <> 'market'] + ['multi_contract_market', 'market']
    res.columns = res.columns.reorder_levels(new_level_order)
    res = res.sort_index(axis=1)
    return res


temp_posbounds = atd.get_posbounds_all()
net_in_posmans = get_net_in_posman_all()
multi_contracts = atd.get_multi_contracts()
desired_posbounds = add_multi_contract_market_level(desired_posbounds, multi_contracts)
scaled_positions = add_multi_contract_market_level(scaled_positions, multi_contracts)

# mkt specific data
slim_pre_carveout = atd.get_softlimit(mkt)
slim = _get_slim_for(get_dataservice(apis=[StaticDataRepository]), mkt)

# filter down to market and forward fill for nice plotting
market_pos = scaled_positions.xs(mkt, level='multi_contract_market', axis=1).ffill(limit=5)
market_desired_posbounds = desired_posbounds.xs(mkt, level='multi_contract_market', axis=1).ffill(limit=5)

# drop strats with no historical positions (why are they here in the first place?)
market_pos = market_pos.replace(0, np.nan).dropna(how='all', axis=1)
assert len(market_pos.columns) > 0, 'no strategies with non-zero positions in ' + mkt
market_desired_posbounds = market_desired_posbounds.reindex(market_pos.columns, axis=1)

# apply temp and net with some hackery to get temp posbounds to apply on positions, and ignore long/short differentiation
market_pos_with_temp = posbound_functions.apply_temp_posbounds(market_pos, temp_posbounds).xs('long', level=-1, axis=1).abs() * market_pos.applymap(np.sign)
market_pos_with_temp_and_net = apply_net_in_posmans(market_pos_with_temp, net_in_posmans)
market_desired_posbounds_with_temp = posbound_functions.apply_temp_posbounds(market_desired_posbounds, temp_posbounds).xs('long', level=2, axis=1)
market_desired_posbounds_with_temp_and_net = apply_net_in_posmans(market_desired_posbounds_with_temp, net_in_posmans)

# info table 
info = pd.DataFrame(index=[mkt], data=[[slim_pre_carveout - slim, slim]], columns=['vol carveouts', 'slim'])
info.index.name = 'market'
info

#### Sum of desired posbounds through time


def plot_posbound_history(market_positions, market_desired_posbounds, slim, lookback, **plotting_kwargs):
        
    sum_posbounds = market_desired_posbounds.sum(axis=1).tail(lookback)
    long_pos = market_positions[market_positions > 0].sum(axis=1).tail(lookback)
    short_pos = market_positions[market_positions < 0].sum(axis=1).tail(lookback)
    net_pos = long_pos + short_pos

    fig, ax = plt.subplots(**plotting_kwargs)

    sum_posbounds.plot(ax=ax, color='darkblue', label='desired posbounds')
    sum_posbounds.multiply(-1).plot(ax=ax, color='darkblue', label='_')
    long_pos.plot(ax=ax, kind='area', color='lightblue', label='desired l/s positions')
    short_pos.plot(ax=ax, kind='area', color='lightblue', label='_')
    net_pos.plot(ax=ax, color='black', linewidth=0.5, label='desired net position')

    # add slim lines
    xlims = ax.get_xlim()
    ax.axhline(0, linewidth=0.5, color='black')
    ax.hlines(slim, *xlims, linestyle='--', label='slim')
    ax.hlines(-slim, *xlims, linestyle='--', label='_')
    ax.set_xlim(*xlims)

    # sort out ylims which area charting screws up
    ylim = max(sum_posbounds.max(), net_pos.max(), slim)
    ax.set_ylim(-ylim * 1.2, ylim * 1.2)

    ax.legend(bbox_to_anchor=(1, 0.5), loc='center left')
    plt.tight_layout()


plot_posbound_history(market_pos_with_temp_and_net, market_desired_posbounds_with_temp_and_net, slim, lookback=TIMESERIES_CHART_LOOKBACK, figsize=(10, 6))

#### Distribution of positions over posbound window


def plot_position_distribution(market_positions, slim, lookback, **plotting_kwargs):
    fig, ax = plt.subplots(**plotting_kwargs)
    market_positions.sum(axis=1).tail(260 * 3).hist(bins=40)
    vlim = ax.get_ylim()
    ax.vlines(slim, *vlim, linestyle='--')
    ax.vlines(-slim, *vlim, linestyle='--')
    ax.axvline(0, color='black')
    # ax.vlines(current_pos,*vlim,linewidth=3,color='darkred')
    ax.set_ylim(*vlim)
    ax.legend(['_', '+- slim', '_', 'current pos', 'dist of net positions'])
    ax.grid(False)
    plt.title('distribution of net positions')


plot_position_distribution(market_pos_with_temp_and_net, slim, lookback=POS_DIST_CHART_LOOKBACK, figsize=(10, 6))

#### Posbounds per strategy


def plot_posbounds_current(market_positions_with_temp, market_desired_posbounds_with_temp_and_net, market_desired_posbounds, **plotting_kwargs):

    current_pos = market_positions_with_temp.iloc[-1]
    current_posbounds = market_desired_posbounds_with_temp_and_net.iloc[-1]
    current_posbounds_pre_temp_and_net = market_desired_posbounds.iloc[-1]
    
    strat_order = current_posbounds_pre_temp_and_net.sort_values(ascending=False).index
    
    fig, ax = plt.subplots(**plotting_kwargs)
    current_posbounds_pre_temp_and_net.reindex(strat_order).to_frame().plot(kind='bar', ax=ax, alpha=0.3)
    current_posbounds_pre_temp_and_net.reindex(strat_order).multiply(-1).to_frame().plot(kind='bar', ax=ax, label='_', alpha=0.3)
    current_posbounds.reindex(strat_order).to_frame().plot(kind='bar', ax=ax)
    current_posbounds.reindex(strat_order).to_frame().multiply(-1).plot(kind='bar', ax=ax, label='_')
    current_pos.reindex(strat_order).plot(kind='line', marker='s', linestyle='None', markerfacecolor='black', markeredgecolor='black', ax=ax)
    ax.legend(['desired position pre temp and net', 'desired posbound pre temp and net', '_', 'desired posbound'])
    ax.axhline(color='grey', zorder=0, linewidth=0.5)
    lim = max(np.abs(ax.get_ylim()))
    # ax.set_ylim([-lim,+lim])
    # ax.set_title('desired posbounds broken out to contract(?) level')   
    ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
    plt.tight_layout()

  
plot_posbounds_current(market_pos_with_temp, market_desired_posbounds_with_temp_and_net, market_desired_posbounds, figsize=(10, 6))
