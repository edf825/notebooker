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
EXCL_STRATS = ['CMBS', 'RVMBS', 'UIRS', 'UCBOND', 'FTREND', 'FIVOL', 'UXENER', 'FSETT']  # alt markets strategies - but we probably DO want FTREND futures markets
INCL_INSIGHT_STRATS = False
# -

# imports
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
from pm.monitoring.caching import pm_cache_enable
import pm.monitoring.positions as positions_functions
import pm.data.strategies as pds

with pm_cache_enable():
    positions = positions_functions.get_all_market_positions()

# get positions and exclude manually excluded and insight only strats
positions = positions.loc(axis=1)[positions.abs().sum() > 0]
positions = positions.loc(axis=1)[~positions.columns.get_level_values(0).isin(EXCL_STRATS)]
positions = positions.loc(axis=1)[positions.columns.get_level_values(0).isin(pds.get_strategies(include_insight_only=INCL_INSIGHT_STRATS))]

# work out markets we care about
multi_contracts = atd.get_multi_contracts()
mkts = positions.columns.get_level_values(1).unique().tolist()
mkts = list(set([multi_contracts.get(x, x) for x in mkts]))
mkts = [str(x) for x in mkts if amd.describe(x)['assetClass'] == 'ASSET_FUTURE']
mkts.sort()


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
    amd.enable_trading_mode(False)
    des = amd.describe(ahl_code)
    return des.get('bbgTicker', des.get('bloomberg', {}).get('original_symbol', np.nan))


def get_participation_values(mkt, lookback=90):

    # get all trades per straetgy per contract for a given market
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
    daily_trade_weighted_participation = participation.multiply(gross_trades_where_volume.div(gross_trades_where_volume.sum(axis=1), axis=0), axis=0).sum(axis=1)

    # max and median calcs
    median_trade = gross_trades.sum(axis=1).replace(0, np.nan).median()
    median_volume = volumes[gross_trades > 0].sum(axis=1).replace(0, np.nan).median()
    max_participation = daily_trade_weighted_participation.max()
    trade_weighted_participation = trade_weighted_participation.sum()

    return {'median_trade':median_trade,
            'median_volume':median_volume,
            'max_participation':max_participation,
            'trade_weighted_participation':trade_weighted_participation}


# +
from ahl.concurrent.futures import hpc_pool
pool = hpc_pool('SPARK', max_workers=32, mem_per_cpu='1g')
test = map(lambda x: pool.submit(get_participation_values, x), mkts)
error_value = {x:np.nan for x in get_participation_values('FTL').keys()}
res = [x.result() if x.exception() is None else error_value for x in test]

# res = list(pool.map(try_get_participation_values, mkts))
resd = dict(zip(mkts, res))
resdf = pd.DataFrame(resd).T
resdf.nlargest(20, 'trade_weighted_participation')

# metadata
market_names = {x:amd.describe(x)['longName'] for x in mkts}
strat_mkt_map = positions.columns.droplevel(2).tolist()
strats_per_mkt = {m:list(set([str(a) for a, b in strat_mkt_map if b in [m] + [x for x, y in multi_contracts.items() if y == m]])) for m in mkts}
num_strats_per_mkt = {m:len(s) for m, s in strats_per_mkt.items()}
res_with_metadata = pd.concat([pd.Series(market_names, name='market_name'),
                               pd.Series(num_strats_per_mkt , name='num_strats'),
                               resdf[['median_trade', 'median_volume', 'max_participation', 'trade_weighted_participation']]], axis=1).reindex(resdf.index)


res_with_metadata[['trade_weighted_participation']].hist()

res_with_metadata[['market_name', 'num_strats', 'median_trade', 'median_volume', 'max_participation', 'trade_weighted_participation']]