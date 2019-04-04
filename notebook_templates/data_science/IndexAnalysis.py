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
    scale_methods = {'normalised': preprocessing.normalize,
                    'totals': lambda x: x}
    # resample to chosen period and scale
    # resample to 1D and reindex to start of data and backfill
    # reindex to end of data
    t_index = pd.DatetimeIndex(start=data.index.min().date(),
                               end=data.index.max().date(),
                               freq='1D')
    agg = pd.DataFrame(data.index.value_counts())\
                .resample(period).sum()\
                .pipe(lambda df_:
                      df_.assign(scale = scale_methods[scale_method]([df_[data.index.name]],
                                                                     norm='l1')[0]))\
                .rename(columns={'scale': period})\
                .drop(data.index.name, axis=1)\
                .pipe(lambda df_:
                          df_.reindex(pd.DatetimeIndex(start=data.index.min().date(),
                                                       end=df_.index.max().date(),
                                                       freq='1D')))\
                .bfill()\
                .reindex(t_index)
    return agg

def resample_index(data, scale_method, resample_periods):
    df_periods = {period: agg_by_period(data, period, scale_method) for period in resample_periods}
    agg = df_periods[resample_periods[0]]
    fig, axes = plt.subplots(nrows=4, ncols=1, sharex=True)
    pylab.rcParams['figure.figsize'] = (15.0, 20.0)
    for i, rp in enumerate(resample_periods):
        df_periods[rp].plot(kind='area', alpha=0.5, stacked=False, linewidth=0, ax=axes[i])
        axes[i].set_title("Aggregated {} count of data resample to {}".format(scale_method, rp))
    plt.show()

resample_index(data, SCALE_METHOD, RESAMPLE_PERIODS)
