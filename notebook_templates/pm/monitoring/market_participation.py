# + {"tags": ["parameters"]}
mkt = 'OBXO'
lookback = 60
# -

# %matplotlib inline
import matplotlib.pyplot as plt
import ahl.tradingdata as atd
import datetime
from datetime import datetime as dt
from ahl.qds import BDH
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import ahl.marketdata as amd
from pandas.tseries.offsets import BDay
from collections import OrderedDict

amd.enable_trading_mode(False)


# +
def get_trades_all_contracts(mkt, include_strats=None, multi_contracts=atd.get_multi_contracts(), start_date=dt.now() - datetime.timedelta(days=30), end_date=dt.now()):
    contracts = [mkt] + [x for x in multi_contracts.keys() if multi_contracts[x] == mkt and x <> mkt]
    strats = atd.get_traded_instruments_by_strategy(include_mkts=contracts, include_strats=include_strats).keys()
    trds = [atd.get_tickets(x, y, start_date=start_date, end_date=end_date).pd for x in strats for y in contracts]
    trds_df = pd.concat(trds, axis=0, keys=[(x, y) for x in strats for y in contracts]).reset_index().rename(columns={'level_0':'strategy', 'level_1':'market', 'level_2':'dt'})
    return trds_df.set_index('dt').sort_index()


def get_daily_trades_per_strat_per_contract(mkt, *args, **kwargs):
    trds = get_trades_all_contracts(mkt, *args, **kwargs).reset_index()
    return trds.groupby([pd.to_datetime(trds['dt'].dt.date), 'strategy', 'delivery'])[['buys', 'sells']].sum()


def get_bloomberg_volumes(contract_tickers):
    volumes = BDH(contract_tickers, 'VOLUME')
    volumes = pd.concat([volumes[x]['VOLUME'] for x in contract_tickers], axis=1, keys=contract_tickers)
    volumes = volumes.resample('B').last().fillna(0)
    volumes.columns = contract_tickers
    return volumes


def get_bloomberg_ticker(ahl_code):
    des = amd.describe(ahl_code)
    return des.get('bbgTicker', des.get('bloomberg', {}).get('original_symbol', np.nan))

# -

# get all trades per strategy per contract for a given market
trades = get_daily_trades_per_strat_per_contract(mkt, start_date=dt.now() - BDay(lookback + 20))  # additional 20 days for rolling average

# calculate daily gross trades going through each contract
gross_trades = trades.abs().sum(axis=1).groupby(level=[0, 2]).sum().unstack().fillna(0).resample('B').sum()
gross_trades = gross_trades.loc(axis=1)[gross_trades.tail(lookback).sum() > 0]
gross_trades = gross_trades.tail(lookback)

# need to map contracts to bloomberg codes so we can get volumes - see contract_ticker_check to see that this mapping seems to be vaguely sensible
contracts = gross_trades.columns.tolist()
ahl_codes = ['_'.join(['FUT', mkt, str(x)[:6]]) for x in contracts]
bbg_tickers = [get_bloomberg_ticker(x) for x in ahl_codes]
na_tickers = [x for x, y in zip(ahl_codes, bbg_tickers) if y is np.nan]
assert len(na_tickers) == 0, 'bloomberg ticker(s) not available for ' + ', '.join(na_tickers)

# now get volumes
volumes = get_bloomberg_volumes(bbg_tickers)
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
    gross_trades_per_strategy = trades.xs(contract, level='delivery').abs().sum(axis=1).unstack().reindex(volumes.index)
    contract_volumes = volumes[contract]
    participation_per_strategy = gross_trades_per_strategy.div(contract_volumes, axis=0)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(contract)
    contract_volumes.rename('contract volume').to_frame().plot(kind='bar', alpha=0.3, ax=ax)
    gross_trades_per_strategy.plot(kind='bar', stacked=True, ax=ax)   
    ax.set_xticklabels(contract_volumes.index.astype(str).values)
    plt.tight_layout()