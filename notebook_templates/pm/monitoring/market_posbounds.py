# + {"tags": ["parameters"]}
mkt = 'NIS'
# -

# imports
# %matplotlib inline
import matplotlib.pyplot as plt
import pm.monitoring as monitoring
from pm.monitoring.caching import pm_cache_enable
import ahl.tradingdata as atd
import pandas as pd
import numpy as np
from ahl.positionmanager.api import get_dataservice
from ahl.positionmanager.repositories import StaticDataRepository, ReadOnlyPositionRepository
from ahl.positionmanager.posbounds_calculation_service import PosboundCalculator, create_strategy_data, _get_manual_posbounds_for, _get_slim_for
import ahl.logging as logging
from collections import OrderedDict
import pm.monitoring.posbound as posbound_functions
import pm.monitoring.multicontracts as multicontract_functions
import pm.monitoring.slim as slim_functions


TIMESERIES_CHART_LOOKBACK = 5 * 260
POS_DIST_CHART_LOOKBACK = 3 * 260

ds = get_dataservice(apis=[StaticDataRepository, ReadOnlyPositionRepository])

# Load up initial data
with pm_cache_enable():
    positions = monitoring.get_all_market_positions()
    max_sim_signal = monitoring.get_max_signal_sim()
    fund_mults_and_constraints = monitoring.get_fund_mults_and_constraints()

# Compute market-level posbounds
desired_posbounds = max_sim_signal * fund_mults_and_constraints
strat_mkt_positions = positions.sum(level=['strategy', 'market'], axis=1, min_count=1)
scaled_positions = strat_mkt_positions * fund_mults_and_constraints

temp_posbounds = atd.get_posbounds_all()
nettable_strategy_markets = posbound_functions.get_nettable_strategy_markets()
multi_contracts = atd.get_multi_contracts()
desired_posbounds = multicontract_functions.add_multi_contract_market_level(desired_posbounds, multi_contracts)
scaled_positions = multicontract_functions.add_multi_contract_market_level(scaled_positions, multi_contracts)

# mkt specific data
slim_pre_carveout = atd.get_softlimit(mkt)
slim = slim_functions.get_market_softlimits([mkt])[mkt]

# filter down to market and forward fill for nice plotting
market_pos = scaled_positions.xs(mkt, level='multi_contract_market', axis=1).ffill(limit=5)
market_desired_posbounds = desired_posbounds.xs(mkt, level='multi_contract_market', axis=1).ffill(limit=5)

# drop strats with no historical positions (why are they here in the first place?)
market_pos = market_pos.replace(0, np.nan).dropna(how='all', axis=1)
assert len(market_pos.columns) > 0, 'no strategies with non-zero positions in ' + mkt
market_desired_posbounds = market_desired_posbounds.reindex(market_pos.columns, axis=1)

# apply temp and net with some hackery to get temp posbounds to apply on positions, and ignore long/short differentiation
market_pos_with_temp = posbound_functions.apply_temp_posbounds(market_pos, temp_posbounds).xs('long', level=-1, axis=1).abs() * market_pos.applymap(np.sign)
market_pos_with_temp_and_net = posbound_functions.remove_nettable_strategy_market_posbounds(market_pos_with_temp, nettable_strategy_markets)
market_desired_posbounds_with_temp = posbound_functions.apply_temp_posbounds(market_desired_posbounds, temp_posbounds).xs('long', level=2, axis=1)
market_desired_posbounds_with_temp_and_net = posbound_functions.remove_nettable_strategy_market_posbounds(market_desired_posbounds_with_temp, nettable_strategy_markets)

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


plot_posbound_history(market_pos_with_temp_and_net, market_desired_posbounds_with_temp_and_net, slim, lookback=TIMESERIES_CHART_LOOKBACK, figsize=(10, 8))

#### Distribution of positions over posbound window


def plot_position_distribution(market_positions, slim, lookback, **plotting_kwargs):
    fig, ax = plt.subplots(**plotting_kwargs)
    market_positions.sum(axis=1).tail(lookback).hist(bins=40)
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

#### Desired posbounds per strategy


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

#### Current state of things according to posman
# Note the desired posbounds shown here are slightly different from those above - they are based off the weekly max signal calc and combined with a buffer.


# +
def get_posbound_table(mkt, dataservice):

    with logging.log_utils.set_logging_level(logging.logging.WARN):
        market_family = ds.get_market_family(mkt)
        slim = _get_slim_for(ds, mkt)
        max_signal_data = ds.get_strategy_data_related_to(market_family=market_family)
        strategy_data = create_strategy_data(dataservice=ds, max_signal_data=max_signal_data)
        temp_posbounds = _get_manual_posbounds_for(dataservice=ds, market_family=market_family)
    
        calcs = PosboundCalculator(slim, strategy_data, temp_posbounds)
        final_posbounds = calcs.calculate_strategy_posbounds()
    
        # get everything into dict format with keys as (strat,mkt) pairs
        max_signals = {x:float(y.max_signal) for x, y in strategy_data.iteritems()}
        buffer_ratios = {x:float(y.buffer_ratio) for x, y in strategy_data.iteritems()}
        fms = {x:float(y.fm) for x, y in strategy_data.iteritems()}
        net_in_posmans = {x:y.net_in_posman for x, y in strategy_data.iteritems()}
        current_positions = {x:float(y.current_position) for x, y in strategy_data.iteritems()}
        desired_posbounds = {x:y.calculate_desired_posbound() for x, y in calcs.strategies.iteritems()}
        temp_long_posbounds = {x:y.long_value for x, y in temp_posbounds.iteritems()} if len(temp_posbounds) > 0 else {}
        temp_short_posbounds = {x:y.short_value for x, y in temp_posbounds.iteritems()} if len(temp_posbounds) > 0 else {}
        final_long_posbounds = {x:y[0] for x, y in final_posbounds.iteritems()} 
        final_short_posbounds = {x:y[1] for x, y in final_posbounds.iteritems()}
        
    # combine to results
    res_df = pd.DataFrame(OrderedDict([
        ('current_pos', current_positions),
        ('max_signal', max_signals),
        ('buffer_ratio', buffer_ratios),
        ('fm', fms),
        ('desired_posbound', desired_posbounds),
        ('net_in_posman', net_in_posmans),
        ('temp_long_posbound', temp_long_posbounds),
        ('temp_short_posbound', temp_short_posbounds),
        ('final_long_posbound', final_long_posbounds),
        ('final_short_posbound', final_short_posbounds),
        ]))
    
    # add in slim
    res_df['slim'] = slim
    strat_order = res_df.groupby(res_df.index.get_level_values(0))['desired_posbound'].max().sort_values(ascending=False).index.tolist()
    return res_df[res_df['max_signal'] <> 0].T[strat_order].T


def plot_posbound_table(posbound_table, **plotting_kwargs):
    res = posbound_table.reset_index()
    res.index = res['level_0'] + '_' + res['level_1']
    fig, ax = plt.subplots(**plotting_kwargs)
    res[['desired_posbound']].plot(ax=ax, kind='bar', alpha=0.3)
    (-res[['desired_posbound']]).plot(ax=ax, kind='bar', alpha=0.3)
    res[['final_long_posbound']].plot(ax=ax, kind='bar')
    (-res[['final_short_posbound']]).plot(ax=ax, kind='bar')
    res['current_pos'].plot(kind='line', marker='s', linestyle='None', markerfacecolor='black', markeredgecolor='black', ax=ax, label='current_pos')
    ax.legend(['current', '_', 'desired posbound with buffer', '_', 'final posbound', '_'])
    ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
    fig.tight_layout()
    fig.show()

# -


res = get_posbound_table(mkt, ds)
res = res[res['fm']>0] # only strats with actual allocation
# formatting so table fits in frame
res_formatted = res.rename(columns={'temp_long_posbound':'temp_long',
                            'temp_short_posbound':'temp_short',
                            'final_long_posbound':'final_long',
                            'final_short_posbound':'final_short'})
res_formatted = res_formatted.fillna('-')
res_formatted.style.format({'max_signal':'{:.1f}',
                            'fm':'{:.2f}',
                            'net_in_posman':'{:,.0f}'})

plot_posbound_table(res, figsize=(10, 6))
