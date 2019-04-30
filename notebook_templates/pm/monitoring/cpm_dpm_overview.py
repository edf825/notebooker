# + {"tags": ["parameters"]}
lookback = 3*260
delay = 5
lookthrough_map = {
    'DMC0':['AGE1'],
    'DES1':['DES1'],
    'ADC8':['AGE1'],
    'DTW0':['AGE1'],
    'OPA3':['AGE2'],
    }
prod_alloc_data_substitutes = {'EFF0': 'EFN1'} # we view EFN1 as a mac only, for some reason, so don't have product returns for it
# -

# %matplotlib inline
import pandas as pd
import ahl.returnbreakdown.api as arb
import risk.data.funds as rdf
import pm.data.fund_allocations as fa
from datetime import datetime as dt
import numpy as np
from collections import OrderedDict
from IPython.display import HTML
from pandas.tseries.offsets import BDay
import ahl.tradingdata as atd

end_dt = dt.now().date() - BDay(delay)
start_dt = end_dt-BDay(lookback)

def get_current_dpm(product):
    raw_gearing_and_allocs = fa._get_raw_gearing_and_allocation_data(product)
    raw_gearing_and_allocs['fund_id_child'] = raw_gearing_and_allocs['fund_id_child'].replace('None',product)
    current_gearing_and_allocs = raw_gearing_and_allocs[
            (raw_gearing_and_allocs['start_date']<dt.now()) & \
            (raw_gearing_and_allocs['end_date']>dt.now())].groupby(['fund_id_child','gearing_name'])['gearing_value'].last()

    non_zero_allocs = current_gearing_and_allocs.loc[:,'Target'].replace(0,np.nan).dropna().index.tolist()
    current_dpms = set(current_gearing_and_allocs.loc[non_zero_allocs, 'Adjustment for Exposure Dilution'])
    assert len(current_dpms) == 1
    return list(current_dpms)[0]

def get_raw_prod_allocs_with_lookthrough(raw_allocs, lookthroughs=[]):
    allocs = raw_allocs.to_dict()
    allocs_with_lookthrough = []
    for m,w in allocs.items():
        if m in lookthroughs:
            strat_allocs = fa.get_mac_strategy_geared_allocations(m,latest=True)
            for s,w1 in strat_allocs.items():
                allocs_with_lookthrough.append(((m,s),w * w1))
        else:
            allocs_with_lookthrough.append(((m,None),w))
    return pd.Series(data=dict(allocs_with_lookthrough).values(),
                     index=pd.MultiIndex.from_tuples(dict(allocs_with_lookthrough).keys())).sort_index()

def calculate_diversification_benefit(sim_rets_df,allocs):
    """
    - Gives the *additional* leverage needed to hit target vol of 15 given the provided weights
    - Works on either prods or macs (but for macs have to add a mac level to returns dataframe
    - Enter the pre-dpm (but post vol scaling) weights to get a proposed dpm
    - Sim rets get vol normalised so can use ahl.returnbreakdown sim rets directly
    """
    mac_strats = allocs.index.tolist()
    wgts = allocs.values.tolist()
    sub_rets = [sim_rets_df.loc(axis=1)[m].sum(axis=1) if s is np.nan
                else sim_rets_df.loc(axis=1)[m,s].sum(axis=1)
                for (m,s) in mac_strats]
    ret_df = pd.concat(sub_rets,axis=1,keys=mac_strats)
    vols = ret_df.rolling(5).sum().std() * 7
    vol_normalised_rets = ret_df.div(vols,axis=1) * 15
    alloc_rets_df = vol_normalised_rets.multiply(wgts,axis=1)
    resulting_vol = alloc_rets_df.sum(axis=1).rolling(5).sum().std() * 7
    diversification_benefit = 15 / resulting_vol
    return diversification_benefit

def get_risk_product_list(incl_groups,grains):
    fund_info = rdf.get_rosa_fund_list().set_index('FUNDID',drop=False)
    fund_info['ahl'] = rdf.get_ahl_fund_mask(fund_info)
    # ignore secondary share classes (termed AHL Fund Class)
    res = fund_info[
        (fund_info['ahl']==True) & \
        (fund_info['FUNDTYPE'] <> 'AHL Fund Class') & \
        (fund_info['FUNDLIQGROUPS'].isin(incl_groups)) & \
        (fund_info['MACGRAINFUNDS'].isin(grains))
        ]
    return res

# main functions
def calc_proposed_dpm(product,start_dt,end_dt,alloc_data_substitutes={}):
    rets = arb.get_product_returns(product,'sim').resample('B').sum()
    assert rets.index[-1]>end_dt, 'returns only calculated up to {}'.format(rets.index[-1].strftime('%Y-%m-%d'))
    rets = rets.loc[start_dt:end_dt]
    geared_allocs = fa.get_direct_geared_allocations(alloc_data_substitutes.get(product,product),latest=True)
    raw_allocs = geared_allocs / geared_allocs.sum()
    lookthrough_allocs = get_raw_prod_allocs_with_lookthrough(raw_allocs, lookthrough_map.get(product, []))
    return calculate_diversification_benefit(rets,lookthrough_allocs)

def calc_proposed_cpm(mac,start_dt,end_dt):
    rets = arb.get_mac_returns(mac,'sim').resample('B').sum()
    assert rets.index[-1]>end_dt, 'returns only calculated up to {}'.format(rets.index[-1].strftime('%Y-%m-%d'))
    rets = rets.loc[start_dt:end_dt]
    rets = pd.concat([rets],keys=[mac],names=['mac'],axis=1)
    raw_allocs = fa.get_mac_strategy_allocations(mac,latest=True)
    raw_allocs.index = pd.MultiIndex.from_tuples(zip([mac]*len(raw_allocs),raw_allocs.index))
    return calculate_diversification_benefit(rets,raw_allocs)


#### DPMs

prod_info = get_risk_product_list(incl_groups=['Dimension', 'Diversified', 'Evolution', 'Evolution Frontier'],
                                  grains=['Product','MacAndProduct'])
prods = prod_info.index.astype(str).tolist()
prod_groups = prod_info['FUNDLIQGROUPS'].tolist()
current_dpms = pd.Series([get_current_dpm(prod_alloc_data_substitutes.get(x, x)) for x in prods], prods)

from ahl.concurrent.futures import hpc_pool
pool = hpc_pool('PROCESS') #hpc_pool('SPARK',max_workers=60,mem_per_cpu='1g')
dpm_pool_submit = map(lambda x: pool.submit(calc_proposed_dpm, x, start_dt, end_dt, prod_alloc_data_substitutes), prods)
dpm_pool_res = [x.result() if x.exception() is None else np.nan for x in dpm_pool_submit]
dpm_pool_errors = [(p,x.exception()) for p,x in zip(prods,dpm_pool_submit) if x.exception() is not None]

proposed_dpms = pd.Series(dict(zip(prods,dpm_pool_res)))
dpm_diffs = proposed_dpms / current_dpms - 1
dpm_res = pd.concat([current_dpms,proposed_dpms,dpm_diffs],axis=1,keys=['current','proposed','diff'])
dpm_res.index = pd.MultiIndex.from_tuples(zip(prod_groups,prods))
dpm_res = dpm_res.sort_index()
HTML(dpm_res.style.format({'current':'{:,.2f}',
                           'proposed':'{:,.2f}',
                           'diff':'{:,.0%}'}).render().replace('nan%', '-').replace('nan', '-'))

#### CPMs

mac_list = sorted([x for x,y in atd.get_funds_all().items() if y.style<>'DEAD'])
current_cpms = pd.Series([fa.get_mac_gearing(x,latest=True) for x in mac_list],mac_list)

cpm_pool_submit = map(lambda x: pool.submit(calc_proposed_cpm, x, start_dt,end_dt), mac_list)
cpm_pool_res = [x.result() if x.exception() is None else np.nan for x in cpm_pool_submit]
cpm_pool_errors = [(m,x.exception()) for m,x in zip(prods,cpm_pool_submit) if x.exception() is not None]

proposed_cpms = pd.Series(dict(zip(mac_list,cpm_pool_res)))
cpm_diffs =  proposed_cpms / current_cpms - 1

cpm_res = pd.concat([current_cpms,proposed_cpms,cpm_diffs],axis=1,keys=['current','proposed','diff'])
cpm_res = cpm_res.sort_index()
HTML(cpm_res.style.format({'current':'{:,.2f}',
                 'proposed':'{:,.2f}',
                 'diff':'{:,.0%}'}).render().replace('nan%', '-').replace('nan', '-'))


HTML('<i>Toggle code to see {} dpm errors and {} cpm errors</i>'.format(len(dpm_pool_errors),len(cpm_pool_errors)))

# dpm errors
print('\n'.join([' - '.join([p,str(type(e)),str(e)]) for (p,e) in dpm_pool_errors]))

# cpm errors
print('\n'.join([' - '.join([m,str(type(e)),str(e)]) for (m,e) in cpm_pool_errors]))
