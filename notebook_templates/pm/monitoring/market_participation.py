# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 1.0.5
#   kernelspec:
#     display_name: pm_notebook_kernel
#     language: python
#     name: pm_notebook_kernel
# ---

mkt = 'BAM'
lookback = 60

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
    volumes = pd.concat([volumes[x]['VOLUME'] for x in bbg_tickers], axis=1, keys=bbg_tickers)
    volumes = volumes.resample('B').last().fillna(0)
    volumes.columns = contract_tickers
    return volumes


# -

# get all trades per straetgy per contract for a given market
trds = get_daily_trades_per_strat_per_contract(mkt, start_date=dt.now()-BDay(lookback+20)) # additional 20 days for rolling average

# calculate daily gross trades going through each contract
gross_trades_per_contract = trds.abs().sum(axis=1).groupby(level=[0, 2]).sum().unstack().fillna(0).resample('B').sum()
gross_trades_per_contract = gross_trades_per_contract.loc(axis=1)[gross_trades_per_contract.sum() > 0]
gross_trades_per_contract_rolling_mean = gross_trades_per_contract.rolling(20).mean()
gross_trades_per_contract = gross_trades_per_contract.tail(lookback)

# need to map contracts to bloomberg codes so we can get volumes - see contract_ticker_check to see that this mapping seems to be vaguely sensible
contracts = gross_trades_per_contract.columns.tolist()
ahl_codes = ['_'.join(['FUT', mkt, str(x)[:6]]) for x in contracts]
bbg_tickers = [amd.describe(x).get('bbgTicker', np.nan) for x in ahl_codes]

# now get volumes
volumes = get_bloomberg_volumes(bbg_tickers)
volumes.columns = contracts
volumes = volumes.reindex_like(gross_trades_per_contract).fillna(0)

# participation calcs
participation = gross_trades_per_contract.div(volumes, axis=1)
participation = participation.replace(0, np.nan)  # we only care about participation on days when we actually trade 
trade_weighting = gross_trades_per_contract.div(gross_trades_per_contract.sum().sum()) 
trade_weighted_participation = (participation * trade_weighting).sum(axis=1)
daily_trade_weighted_participation = participation.multiply(gross_trades_per_contract.div(gross_trades_per_contract.sum(axis=1),axis=0),axis=0).sum(axis=1)

# max and median calcs
median_trade = gross_trades_per_contract.sum(axis=1).replace(0, np.nan).median()
median_volume = volumes[gross_trades_per_contract > 0].sum(axis=1).median()
max_participation = trade_weighted_participation.quantile(0.95)
trade_weighted_participation = trade_weighted_participation.sum()

pd.Series(OrderedDict([
    ('median daily trade',median_trade),
    ('median market volume',median_volume),
    ('trade weighted participation',trade_weighted_participation),
    ('max participation',max_participation_overall),
])).rename('').to_frame()

# #### Participation per contract

fig,ax = plt.subplots(figsize=(10,6))
participation.fillna(0).tail(lookback).plot(ax=ax)
daily_trade_weighted_participation.plot(ax=ax,label='daily trade weighted',color='black',alpha=.2,linewidth=0,kind='area')
plt.legend()
plt.tight_layout()

daily_trade_weighted_participation.hist()

# #### 20d average volumes per contract

gross_trades_per_contract_rolling_mean.dropna().plot(kind='area',stacked=True,figsize=(10,6),linewidth=0)

# #### Strategy volumes per contract

# per contract plots
for contract in contracts:
    gross_trades_per_strategy = trds.xs(contract, level='delivery').abs().sum(axis=1).unstack().reindex(volumes.index)
    contract_volumes = volumes[contract]
    participation_per_strategy = gross_trades_per_strategy.div(contract_volumes, axis=0)
    fig, ax = plt.subplots(figsize=(10,6))
    ax.set_title(contract)
    contract_volumes.rename('contract volume').to_frame().plot(kind='bar', alpha=0.3, ax=ax)
    gross_trades_per_strategy.plot(kind='bar', stacked=True, ax=ax)   
    ax.set_xticklabels(contract_volumes.index.astype(str).values)
    plt.tight_layout()
