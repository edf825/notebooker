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
mkt = 'PPUS'
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

# Load up initial data
with pm_cache_enable():
    positions = monitoring.get_all_market_positions()
    max_sim_signal = monitoring.get_max_signal_sim()
    fund_mults_and_constraints = monitoring.get_fund_mults_and_constraints()

# Compute market-level posbounds
desired_posbounds = max_sim_signal * fund_mults_and_constraints
strat_mkt_positions = positions.sum(level=['strategy', 'market'], axis=1, min_count=1)
scaled_positions = strat_mkt_positions * fund_mults_and_constraints

market_pos = scaled_positions.xs(mkt, level='market', axis=1)
market_pos = market_pos[market_pos.ne(0.).any(axis=1).idxmax():]
mpos = pd.DataFrame()
mpos['long'] = market_pos[market_pos > 0.].sum(axis=1)
mpos['short'] = market_pos[market_pos < 0.].sum(axis=1)

desired_posbounds = desired_posbounds[desired_posbounds.ne(0.).any(axis=1).idxmax():]
mposbounds = desired_posbounds.xs(mkt, level='market', axis=1)
mposbounds_sum = mposbounds[market_pos.first_valid_index():].sum(axis=1)
try:
    slim = atd.get_softlimit(mkt)
except ValueError:
    logger.warn('No SLIM for {}, setting to zero'.format(mkt))
    slim = 0

#### Drill down to individual market

fig, ax = plt.subplots(figsize=(10, 6))
mposbounds_sum.plot(ax=ax,color='darkblue',label='desired_long_posbound')
mpos['long'].plot(ax=ax,color='lightblue',label='desired_long_pos')
mpos['short'].plot(ax=ax,color='orange',label='desired_net_pos')
mpos.sum(axis=1).plot(ax=ax,color='darkgrey',linewidth=0.5,label='desired_short_pos')
mposbounds_sum.multiply(-1).plot(ax=ax,color='red',label='desired_short_posbound')
xlims = ax.get_xlim()
ax.hlines(slim, *xlims, linestyle='--',label='+slim')
ax.hlines(-slim, *xlims, linestyle='--',label='-slim')
ax.set_xlim(*xlims)
ax.legend(bbox_to_anchor=(1,0.5), loc='center left')
ax.set_title('posbound = 99% quantile of abs net pos')
ax.set_title('l/s posbound = 99% quantile of l/s gross pos')
plt.tight_layout()

# Note: in the case of a market traded across multiple strats, our desired posbounds may look high relative to desired positions.
# This is because we're calculating desired posbound as the sum of the underlying per-strat posbounds, rather than as the max signal on the combined positions.
# This is very conservative - should we run max signal on the comb positions instead? Look at e.g. WHC

current_pos = mpos.iloc[-1].sum()

DEFAULT_POSBOUND_WINDOW = 260 * 3
fig,ax = plt.subplots(figsize=(10,5))
mpos['long'].tail(DEFAULT_POSBOUND_WINDOW).hist(bins=20)
mpos['short'].tail(DEFAULT_POSBOUND_WINDOW).hist(bins=20)
vlim = ax.get_ylim()
ax.vlines(slim,*vlim,linestyle='--')
ax.vlines(-slim,*vlim,linestyle='--')
ax.vlines(current_pos,*vlim,linewidth=3,color='darkred')
ax.set_ylim(*vlim)
ax.legend(['+- slim','_','current pos','dist of gross long positions','dist of gross short positions'])
plt.title('dist of long vs short positions')

# Note this is sum of long/short positions across strats so is a loose measure in the case of multiple strats

#### Per strategy section

temp_posbounds = pd.DataFrame(atd.get_posbounds_all()).stack().apply(pd.Series)[['temp_long', 'temp_short']].dropna(how='all').abs()
multi_contracts = atd.get_multi_contracts()
sub_mkts = list({mkt} | {k for k, v in multi_contracts.items() if v == mkt})

spos = scaled_positions.ffill().loc[:, (slice(None), sub_mkts)].iloc[-1]
sposbounds = desired_posbounds.ffill().loc[:, (slice(None), sub_mkts)].iloc[-1]

stempposbounds = temp_posbounds.loc[sub_mkts].reorder_levels([1, 0]).reindex(index=sposbounds.index)

spos_ls = pd.DataFrame(index=spos.index)
spos_ls['long'] = spos[spos>0]
spos_ls['short'] = spos[spos<0]
spos1 = spos_ls.sum(level='strategy')

# TODO: check if long/short are mirrors or not
sposbounds1 = pd.DataFrame()
sposbounds1['long'] = sposbounds.sum(level='strategy')
sposbounds1['short'] = -1 * sposbounds1['long']

#### Calculate implied temp posbounds at the agg'd mkt level

sposbounds_with_temp = pd.DataFrame()
sposbounds_with_temp['long'] = sposbounds.sum(level='strategy')
sposbounds_with_temp['short'] = -1 * sposbounds_with_temp['long']
stempposbounds1 = sposbounds_with_temp[sposbounds_with_temp != sposbounds1].reindex_like(sposbounds1).abs()

#### Plot at market-level

fig, ax=plt.subplots(figsize=(10,5))
sposbounds1['long'].plot(kind='bar',ax=ax,label='desired_posbounds')
sposbounds1['short'].plot(kind='bar',ax=ax)
spos1.sum(axis=1).reindex(sposbounds1.index).plot(kind='line',marker='s',linestyle='None',markerfacecolor='black',markeredgecolor='black',ax=ax)
ax.axhline(linestyle='--',color='grey',zorder=0)
for t, x in enumerate(stempposbounds1.values):
    ax.hlines([x[0],-x[1]],t-0.3,t+0.3,linestyle='--')
ax.legend(['current desired pos','_','temp posbounds','_','_'])
lim = max(np.abs(ax.get_ylim()))
ax.set_ylim([-lim,+lim])
ax.set_title('desired posbounds broken out to market level')

#### Plot at contract-level

fig,ax=plt.subplots(figsize=(10,5))
sposbounds.plot(kind='bar',ax=ax,label='desired_posbounds')
(-1 * sposbounds).plot(kind='bar',ax=ax)
spos.reindex(sposbounds.index).plot(kind='line',marker='s',linestyle='None',markerfacecolor='black',markeredgecolor='black',ax=ax)
ax.axhline(linestyle='--',color='grey',zorder=0)
for t, x in enumerate(stempposbounds.values):
    ax.hlines([x[0],-x[1]],t-0.3,t+0.3,linestyle='--')
ax.legend(['current desired pos','_','temp posbounds','_','_'])
lim = max(np.abs(ax.get_ylim()))
ax.set_ylim([-lim,+lim])
ax.set_title('desired posbounds broken out to contract level')
