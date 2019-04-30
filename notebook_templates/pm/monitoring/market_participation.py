# + {"tags": ["parameters"]}
mkt = 'FTL'
lookback = 60
include_insight_strats = False
# -

# %matplotlib inline
import matplotlib.pyplot as plt
import ahl.tradingdata as atd
from datetime import datetime as dt
import pandas as pd
import numpy as np
import ahl.marketdata as amd
from pandas.tseries.offsets import BDay
from collections import OrderedDict
import pm.data.strategies as pds
import pm.monitoring.multicontracts as pmmc
import pm.monitoring.trades as pmt
import pm.monitoring.volume as pmv
from ahl.db import DOTS_DB


# list of strats we care about
strats = pds.get_strategies(include_insight_only=include_insight_strats)

# get all trades per strategy per contract for a given market
multi_contracts = atd.get_multi_contracts()
contracts_for_market = pmmc.get_markets_in_family(mkt, multi_contracts)
mkt_trades = pmt.get_market_trades(contracts_for_market, start_date=dt.now() - BDay(lookback + 20)).reset_index()  # additional 20 days for rolling average
trades = mkt_trades.groupby([pd.to_datetime(mkt_trades['dt'].dt.date), 'strategy', 'instrument', 'delivery'])[['buys', 'sells']].sum()

# calculate daily gross trades going through each contract
gross_trades = trades.abs().sum(axis=1).groupby(level=['dt', 'delivery']).sum().unstack().fillna(0).resample('B').sum()
gross_trades = gross_trades.loc(axis=1)[gross_trades.tail(lookback).sum() > 0]
gross_trades = gross_trades.tail(lookback)

# need to map contracts to bloomberg codes so we can get volumes - see contract_ticker_check to see that this mapping seems to be vaguely sensible
contracts = gross_trades.columns.tolist()
condense_contract_code = lambda x: x[:-2] if x[-2:]=='00' else x
contracts_condensed = map(condense_contract_code,contracts)

ahl_symbol = amd.describe(mkt)['symbol']
ahl_codes = ['_'.join([ahl_symbol,x]) for x in contracts_condensed]
with amd.features.set_trading_mode(False):
    bbg_tickers = [pmv.get_bloomberg_ticker(x) for x in ahl_codes]
na_tickers = [x for x, y in zip(ahl_codes, bbg_tickers) if y is None]
assert len(na_tickers) == 0, 'bloomberg ticker(s) not available for ' + ', '.join(na_tickers)

# now get volumes
volumes = pmv.get_bloomberg_volumes(bbg_tickers)
volumes.columns = contracts
volumes = volumes.reindex_like(gross_trades)
volumes = volumes.replace(0, np.nan)  # where we have no volume it's probably bloomberg data error, so we ignore

# participation calcs
gross_trades_where_volume = gross_trades[volumes > 0].replace(0, np.nan)
participation = gross_trades_where_volume.div(volumes, axis=1)

trade_weighting = gross_trades_where_volume.div(gross_trades_where_volume.sum().sum())
trade_weighted_participation = (participation * trade_weighting).sum(axis=1)
daily_trade_weighted_participation = participation.multiply(
    gross_trades_where_volume.div(gross_trades_where_volume.sum(axis=1), axis=0), axis=0).sum(axis=1)

# max and median calcs
median_trade = gross_trades.sum(axis=1).replace(0, np.nan).median()
median_volume = volumes[gross_trades > 0].sum(axis=1).replace(0, np.nan).median()
max_participation = daily_trade_weighted_participation.max()
trade_weighted_participation = trade_weighted_participation.sum()

# #### Participation stats

pd.Series(OrderedDict([
    ('market', mkt),
    ('median daily trade', median_trade),
    ('median market volume', median_volume),
    ('trade weighted participation', trade_weighted_participation),
    ('max participation', max_participation),
])).rename('').to_frame().T.style.format({'median daily trade':'{:,.0f}',
                                          'median market volume':'{:,.0f}',
                                          'trade weighted participation':'{:,.2%}',
                                          'max participation':'{:,.2%}'
                                         })

# #### Trading info

# some general info
def df_from_dict(dict_of_dicts):
    return pd.concat({
        k: df_from_dict(v) if all(isinstance(c, dict) for c in v.values()) else pd.Series(v)
        for k, v in dict_of_dicts.viewitems()
    })
def get_sample_times_df(mkt, include_strats=None, multi_contracts=atd.get_multi_contracts()):
    contracts = [mkt] + [x for x in multi_contracts.keys() if multi_contracts[x] == mkt and x <> mkt]
    sample_times = df_from_dict(atd.get_sample_times(include_mkts=contracts,include_strats=include_strats)).unstack()
    sample_times.index.names = ['strategy','instrument','sample_time']
    return sample_times
sample_times = get_sample_times_df(mkt,strats)
sum_max_orders = max_orders = sample_times.groupby(['strategy','instrument'])['max_order'].sum().rename('sum_max_order')
abs_trades = trades.abs().sum(axis=1).replace(0,np.nan).dropna()
deliveries_per_instrument = abs_trades.reset_index().groupby('instrument')['delivery'].unique()
deliveries_per_instrument_str = deliveries_per_instrument.apply(lambda x: ', '.join([condense_contract_code(y) for y in x])).rename('deliveries')
avg_trade_per_strat = abs_trades.groupby(level=['instrument','strategy']).median().rename('median_trade')
res = pd.merge(deliveries_per_instrument_str.reset_index(),avg_trade_per_strat.reset_index(),on='instrument')
res = pd.merge(res,sum_max_orders.reset_index(),on=['strategy','instrument'])
res = res.groupby(['instrument','deliveries','strategy'])[['median_trade','sum_max_order']].last()
res

# #### Daily participation

fig1, ax1 = plt.subplots(1, 2, figsize=(10, 6), sharey=True, gridspec_kw={'width_ratios':[2, 1]})
participation.fillna(0).tail(lookback).plot(ax=ax1[0])
daily_trade_weighted_participation.plot(ax=ax1[0], label='daily trade weighted', color='black', alpha=.2, linewidth=0, kind='area')
daily_trade_weighted_participation.hist(ax=ax1[1], bins=10, orientation='horizontal', density=True)
ax1[0].legend()
plt.tight_layout()

# #### Daily participation, rolling 20 day mean

fig2, ax2 = plt.subplots(1, 2, figsize=(10, 6), sharey=True, gridspec_kw={'width_ratios':[2, 1]})
participation.fillna(0).rolling(20).mean().tail(lookback).plot(ax=ax2[0])
daily_trade_weighted_participation.rolling(20).mean().plot(ax=ax2[0], label='daily trade weighted', color='black', alpha=.2, linewidth=0, kind='area')
daily_trade_weighted_participation.rolling(20).mean().hist(ax=ax2[1], bins=10, orientation='horizontal', density=True)
ax2[0].set_ylim(*ax1[0].get_ylim())
ax2[0].legend()
plt.legend()
plt.tight_layout()

# #### Strategy volumes per contract

# per contract plots
for contract in contracts:
    gross_trades_per_strategy = trades.xs(contract, level='delivery').abs().sum(axis=1).groupby(level=['dt','strategy']).sum().unstack().reindex(volumes.index)
    contract_volumes = volumes[contract]
    participation_per_strategy = gross_trades_per_strategy.div(contract_volumes, axis=0)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(contract)
    contract_volumes.rename('contract volume').to_frame().plot(kind='bar', alpha=0.3, ax=ax)
    gross_trades_per_strategy.plot(kind='bar', stacked=True, ax=ax)   
    ax.set_xticklabels(contract_volumes.index.astype(str).values)
    plt.tight_layout()


##### Sample times / max orders, exlcuding standalone strats

# +
def df_from_dict(dict_of_dicts):
    return pd.concat({
        k: df_from_dict(v) if all(isinstance(c, dict) for c in v.values()) else pd.Series(v)
        for k, v in dict_of_dicts.viewitems()
    })

def get_sample_time_table(mkt,include_standalone_strats=False):
    standalone_strats = [x for (x,) in DOTS_DB.db_query('select strategy_id from dots.standalone_strategy')] if not include_standalone_strats else []
    live_strats = [x for x,y in atd.get_strategies().items() if y.client==True]
    df = df_from_dict(atd.get_sample_times(include_mkts=mkt,include_strats=set(live_strats) - set(standalone_strats))).loc[:,mkt,:,'max_order'].unstack(level=0).sort_index()
    return df.fillna('-')

get_sample_time_table(mkt,include_standalone_strats=False)
# -

# #### General market info

pd.Series(amd.describe(mkt))[['longName','symbol','assetClass','exchangeId','timezoneId']].rename('').to_frame()

# #### Bloomberg tickers

pd.Series(bbg_tickers,contracts_condensed).rename('').to_frame()