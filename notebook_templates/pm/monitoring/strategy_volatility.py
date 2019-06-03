# + {"tags": ["parameters"]}
lookbacks = [1,3,5]
return_overlap = 5
# -

#%matplotlib inline
import pm.data.strategies as pds
import ahl.performance as p
import numpy as np
import pandas as pd
from collections import OrderedDict
import matplotlib.pyplot as plt
from matplotlib.colors import rgb2hex
from matplotlib import cm
from IPython.display import HTML

strats = pds.get_strategies(include_insight_only=False)
vol_targets = [0.15] * len(strats)

strat_rets = [p.get_performance(strategy=x).pd['percent_pnl_std']/100 for x in strats]
strat_ret_df = pd.concat(strat_rets,axis=1,keys=strats)
strat_ret_df = strat_ret_df.resample('B').sum().replace(0,np.nan)

def remove_data_preceding_consecutive_nans(df,num_nans):
    consecutive_nan_screen = df.isnull().rolling(num_nans).sum().ge(num_nans).replace(False,np.nan).bfill()
    return df.fillna(0)[consecutive_nan_screen != 1].copy()

strat_ret_df = remove_data_preceding_consecutive_nans(strat_ret_df,num_nans=20)

def calc_vol_overlapping_returns(df,return_overlap=5,skipna=False):
    return df.rolling(return_overlap).sum()[return_overlap:].std(skipna=skipna) * np.sqrt(260/return_overlap)

vol_over_lookbacks = [calc_vol_overlapping_returns(strat_ret_df.tail(x*260),return_overlap=return_overlap) for x in lookbacks]
vol_over_lookbacks_res = OrderedDict([('vol_{}y'.format(x),vol_over_lookbacks[i]) for i,x in enumerate(lookbacks)])

vol_full_period = calc_vol_overlapping_returns(strat_ret_df,return_overlap=return_overlap,skipna=True)
vol_sub_1y = vol_full_period[strat_ret_df.tail(260).isnull().sum()>0]

res = pd.DataFrame(index=strats,data=OrderedDict(
    [('vol_target',vol_targets),
     ('vol_sub_1y',vol_sub_1y)] +
    [('vol_{}y'.format(x),vol_over_lookbacks[i]) for i,x in enumerate(lookbacks)]))

def get_colormap_function(df, col_subset=None,colormap=cm.RdBu, fix_min=None, fix_max=None, fix_mean=None):
    # to reverse colormap use e.g. matplotlib.cm.RdBu_r as the colormap
    if col_subset is None: col_subset = df.columns
    min_val = df[col_subset].min().min() if fix_min is None else fix_min
    max_val = df[col_subset].max().max() if fix_max is None else fix_max
    mean_val = (max_val - min_val)/ 2 if fix_mean is None else fix_mean
    get_scaled_x = lambda x: np.interp(x,[min_val,mean_val,max_val],[50,128,200])
    get_hex_color = lambda x: rgb2hex(colormap(int(x))) if not np.isnan(x) else '#ffffff'
    return lambda x: 'background-color:{}'.format(get_hex_color(get_scaled_x(x)))

def print_dataframe_heatmap(df,col_subset=None, **kwargs):
    if col_subset is None: col_subset = df.columns
    color_fn = get_colormap_function(df, col_subset=col_subset,**kwargs)
    return df.style.applymap(color_fn, subset=col_subset)

HTML(print_dataframe_heatmap(res, col_subset=res.columns[1:], fix_min=0.05, fix_max=0.20, fix_mean=0.14).format('{:,.0%}').render().replace('nan%', '-'))

for l in lookbacks:
    fig,ax = plt.subplots(figsize=(10,6))
    res['vol_{}y'.format(l)].dropna().sort_values().to_frame().plot(kind='bar',ax=ax)
    ax.axhline(0.15,color='black')
