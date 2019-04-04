# ---
# jupyter:
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: data
#     language: python
#     name: data
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
# ---

# +
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pylab
import seaborn as sns
from sklearn import preprocessing

from ahl.mongo import Mongoose

sns.set_style("whitegrid")
# + {"tags": ["parameters"]}
MONGOOSE_DB = 'research'
LIBRARY_NAME = 'kepler.FLOW'
DATA_NAME = 'KEPLER_TURNOVER'
RESAMPLE_PERIODS = ['1D', '1W', '1M', '1Y']
SCALE_METHOD = 'totals'
INDEX_AS_DATE = False
DATE_COLUMN = 'DATE'

library = Mongoose(MONGOOSE_DB).get_library(LIBRARY_NAME)
data = library.read(DATA_NAME).data
if not INDEX_AS_DATE:
    data.set_index(DATE_COLUMN, inplace=True)

# +
def agg_by_period(data, period, scale_method):
    scale_methods = {'normalised': preprocessing.MinMaxScaler().fit_transform,
                    'totals': lambda x: x}
    # resample to chosen period and scale
    # resample to 1D and backfill so can merge with 1D resampled data
    agg = pd.DataFrame(data.index.value_counts())\
                    .resample(period).sum()\
                    .pipe(lambda df_: df_.assign(scale = scale_methods[scale_method](df_.values)))\
                    .rename(columns={'scale': period})\
                    .drop(data.index.name, axis=1)\
                    .resample('1D').mean()\
                    .bfill()
    return agg

def resample_index(data, scale_method, resample_periods):
    df_periods = {period: agg_by_period(data, period, scale_method) for period in resample_periods}
    agg = pd.merge(df_periods['1D'], df_periods['1W'], how='left', left_index=True, right_index=True)
    agg = pd.merge(agg, df_periods['1M'], how='left', left_index=True, right_index=True)
    # backfill, eg year resample puts data at end of year, so need to backfill to when data starts
    agg = pd.merge(agg, df_periods['1Y'], how='left', left_index=True, right_index=True).bfill()
    fig, axes = plt.subplots(nrows=4, ncols=1, sharex=True)
    pylab.rcParams['figure.figsize'] = (15.0, 20.0)
    agg['1D'].plot(kind='area', alpha=0.5, stacked=False, linewidth=0, ax=axes[0])
    agg['1W'].plot(kind='area', alpha=0.5, stacked=False, linewidth=0, ax=axes[1])
    agg['1M'].plot(kind='area', alpha=0.5, stacked=False, linewidth=0, ax=axes[2])
    agg['1Y'].plot(kind='area', alpha=0.5, stacked=False, linewidth=0, ax=axes[3])
    plt.show()

resample_index(data, SCALE_METHOD, RESAMPLE_PERIODS)
# -
