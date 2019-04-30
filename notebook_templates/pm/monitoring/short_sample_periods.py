# + {"tags": ["parameters"]}
cutoff_minutes_per_sample_period = 20
cutoff_minutes_per_day = 60
# -

# %matplotlib inline
# imports
import ahl.tradingdata as atd
from datetime import datetime as dt
import pandas as pd
import numpy as np
import ahl.marketdata as amd
from pandas.tseries.offsets import BDay
import pm.data.strategies as pds
from ahl.concurrent.futures import hpc_pool
from pm.monitoring.caching import pm_cache_enable
import pm.monitoring.multicontracts as pmmc
import pm.monitoring.positions as pmp
import pm.monitoring.trades as pmt
import pm.monitoring.volume as pmv
import datetime
from ahl.db import DOTS_DB
from IPython.display import HTML

def df_from_dict(dict_of_dicts):
    return pd.concat({
        k: df_from_dict(v) if all(isinstance(c, dict) for c in v.values()) else pd.Series(v)
        for k, v in dict_of_dicts.viewitems()
    })

standalone_strats = [x for (x,) in DOTS_DB.db_query('select strategy_id from dots.standalone_strategy')]
live_strats = [x for (x,) in DOTS_DB.db_query("select strategy_id from dots.strategy where imp <> 'NULL'")]

def get_straplines():
    d = DOTS_DB.db_query("select strategy_id, instrument_id, override_value from dots.trading_override where override_name='CAPTURE_STRATEGY_INSTRUMENT' and override_value is not null",name='pd')
    return pd.Series(d['override_value'].values,pd.MultiIndex.from_tuples(d.reset_index()[['strategy_id','instrument_id']].values.tolist()))

straplined_strat_mkts = get_straplines().index.values.tolist()

all_sample_times = df_from_dict(atd.get_sample_times()).loc[:, :, :, 'max_order']
strat_mkt_times = all_sample_times.index.values.tolist()
strat_mkt_times = [(s,m,t) for s,m,t in strat_mkt_times
                        if s in live_strats
                        and s not in standalone_strats
                        and (s,m) not in straplined_strat_mkts]

next_sample_times = []
next_sample_strats = []
for (s,m,t) in strat_mkt_times:
    future_sample_times = [(s1,t1) for s1, m1, t1 in strat_mkt_times if m1 == m and t1 > t]
    next_sample_time = min([t1 for s1,t1 in future_sample_times]) if len(future_sample_times)>0 else np.nan
    next_sample_strat = [s1 for s1,t1 in future_sample_times if t1==next_sample_time]
    next_sample_times.append(next_sample_time)
    next_sample_strats.append(next_sample_strat)

minutes_to_next_sample = [n.hour*60 + n.minute - (t.hour*60 + t.minute) if n is not np.nan
                          else np.nan for (s,m,t),n in zip(strat_mkt_times,next_sample_times)]

mkts = [m for s,m,t in strat_mkt_times]
links = ['<a href="../market_participation/latest?mkt={}" '
                                           'target="_blank">market notebook</a>'.format(m) for m in mkts]

#### Shortest execution times

res = pd.DataFrame(data=zip(minutes_to_next_sample,next_sample_strats,links),
                   columns=['mins_to_next_sample','next_sample_strats','link'],
                   index=pd.MultiIndex.from_tuples(strat_mkt_times).reorder_levels([1,0,2]))
HTML(res[res['mins_to_next_sample']<cutoff_minutes_per_sample_period].sort_values('mins_to_next_sample').style.render())

#### Minutes traded per day

minutes_traded_per_day = res['mins_to_next_sample'].groupby(level=[0,1]).sum().replace(0,np.nan)
HTML(minutes_traded_per_day[minutes_traded_per_day<cutoff_minutes_per_day].sort_values().rename('').to_frame().style.render())