# ---
# jupyter:
#   celltoolbar: Tags
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: galen.notebooks
#     language: python
#     name: galen.notebooks
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 2
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython2
#     version: 2.7.13
#   papermill:
#     duration: 39.289191
#     end_time: '2019-03-18T10:18:49.040499'
#     environment_variables: {}
#     exception: null
#     input_path: /home/twsadmin/tmp3n3PJ3/7e81df3e9e1bbae40f6b566fdc23d9ad4949e4d8/pm/monitoring/overall_posbounds.ipynb
#     output_path: /home/twsadmin/tmpLvTuvE/pm/monitoring/overall_posbounds/a9ba5ab6-0a94-4d37-b056-04b221c7e356/pm|monitoring|overall_posbounds.ipynb
#     parameters: {}
#     start_time: '2019-03-18T10:18:09.751308'
#     version: 0.16.2
# ---

# + {"papermill": {"duration": 0.025574, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:11.132568", "end_time": "2019-03-18T10:18:11.158142"}, "tags": ["parameters"]}
overallocation_buffer = 0.4

# + {"papermill": {"duration": 4.683134, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:11.170389", "end_time": "2019-03-18T10:18:15.853523"}, "tags": []}
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
from ahl.db import DOTS_DB


# + {"papermill": {"duration": 4.646783, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:15.867294", "end_time": "2019-03-18T10:18:20.514077"}, "tags": []}
# Load up data
with pm_cache_enable():
    positions = monitoring.get_all_market_positions()
    max_sim_signal = monitoring.get_max_signal_sim()

# not from cache while 
fund_mults_and_constraints = monitoring.get_fund_mults_and_constraints()

# +
from ahl.positionmanager.posbounds_calculation_service import _get_slim_for
from ahl.positionmanager.api import get_dataservice

def get_real_slims(mkts):
    ds = get_dataservice()
    effective_slims = [_get_slim_for(ds, x) for x in mkts]
    res = dict(zip(mkts, effective_slims))
    return res
# -

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

def apply_net_in_posmans(desired_posbounds,net_in_posmans):
    res = desired_posbounds
    for col in res.columns:
        strat = col[0]
        mkt = col[1]
        if (strat, mkt) in net_in_posmans:
            res[col] = desired_posbounds[col].clip(upper=0, lower=0)
    return res

# + {"papermill": {"duration": 0.215465, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:20.527587", "end_time": "2019-03-18T10:18:20.743052"}, "tags": []}
temp_posbounds = atd.get_posbounds_all()
multi_contracts = atd.get_multi_contracts()
mkts = positions.columns.get_level_values('market').unique().tolist()
softlimits = get_real_slims(mkts) #atd.get_softlimits_all()
net_in_posmans = get_net_in_posman_all()

# + {"papermill": {"duration": 16.000839, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:20.756000", "end_time": "2019-03-18T10:18:36.756839"}, "tags": []}
# Compute market-level posbounds
desired_posbounds = max_sim_signal * fund_mults_and_constraints
strat_mkt_positions = positions.sum(level=['strategy', 'market'], axis=1, min_count=1)
scaled_positions = strat_mkt_positions * fund_mults_and_constraints
desired_posbounds_with_temp = posbound_functions.apply_temp_posbounds(desired_posbounds, temp_posbounds)
desired_posbounds_with_temp_and_net = apply_net_in_posmans(desired_posbounds_with_temp,net_in_posmans)

# only care about with temp and net now
mkt_desired_posbound_long = posbound_functions.get_market_posbounds(
    desired_posbounds_with_temp_and_net.xs('long', level=2, axis=1), multi_contracts)

mkt_desired_posbound_short = posbound_functions.get_market_posbounds(
    desired_posbounds_with_temp_and_net.xs('short', level=2, axis=1), multi_contracts)

mkt_desired_posbound = pd.concat([mkt_desired_posbound_long,
                                            -1.0 * mkt_desired_posbound_short], axis=1).max(axis=1)

mkt_slim = pd.Series(softlimits).reindex_like(mkt_desired_posbound)

# + {"papermill": {"duration": 10.226623, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:36.767695", "end_time": "2019-03-18T10:18:46.994318"}, "tags": []}
# Compute some derived fields
overallocation = mkt_desired_posbound / mkt_slim.replace(0, np.nan)
market_description = positions_functions.get_position_group_label(mkt_desired_posbound.index, 'client_reporting_label')
sectors = positions_functions.get_position_group_label(mkt_desired_posbound.index, 'sector')
num_strats_traded_in = positions_functions.get_num_strats_traded_in(positions, multi_contracts)

# + {"papermill": {"duration": 0.031642, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:47.011279", "end_time": "2019-03-18T10:18:47.042921"}, "tags": []}
# Create result structure
res = pd.concat([mkt_desired_posbound, mkt_slim, overallocation, 
                 market_description, num_strats_traded_in],
                axis=1,
                keys=['desired_posbound', 'slim', 'overallocation',
                      'market_description', 'num_strats_traded_in'])
res['link'] = res.index.map(lambda market: '<a href="../market_posbounds/latest?mkt={}" '
                                           'target="_blank">market notebook</a>'.format(market))
primary_cols = ['market_description', 'num_strats_traded_in', 'link']
res = res.reindex(columns=primary_cols + res.columns.drop(primary_cols).tolist())
# -

# ### Overallocated markets per sector

# + {"papermill": {"duration": 0.276698, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:47.075153", "end_time": "2019-03-18T10:18:47.351851"}, "tags": []}
fig, ax = plt.subplots()
plotting_functions.plot_sector_overallocation(res['overallocation'], overallocation_buffer,
                                              sectors, ax, '% overallocated mkts')
plt.show()

# + {"papermill": {"duration": 0.018607, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:47.363206", "end_time": "2019-03-18T10:18:47.381813"}, "tags": []}
formatter = {
    'desired_posbound': '{:,.0f}',
    'desired_posbound_with_temp': '{:,.0f}',
    'sum_atd_posbounds': '{:,.0f}',
    'slim': '{:,.0f}',
    'overallocation': '{:,.1f}x',
    'overallocation_with_temp': '{:,.1f}x',
    'num_strats_traded_in': '{:,.0f}'
}
# -

# ### Most overallocated markets

# + {"papermill": {"duration": 0.063247, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:47.413963", "end_time": "2019-03-18T10:18:47.477210"}, "tags": []}
res.nlargest(10, 'overallocation').rename(columns={'net': ''}).style.format(formatter)
# -

# ### Most overallocated markets traded in multiple strats

# + {"papermill": {"duration": 0.033364, "status": "completed", "exception": false, "start_time": "2019-03-18T10:18:47.517146", "end_time": "2019-03-18T10:18:47.550510"}, "tags": []}
res[res['num_strats_traded_in'] > 1].nlargest(10, 'overallocation').rename(columns={'net': ''}).style.format(formatter)
