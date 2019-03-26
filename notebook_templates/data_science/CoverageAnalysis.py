# ---
# jupyter:
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: notebooker_kernel
#     language: python
#     name: notebooker_kernel
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

from ahl.mongo import Mongoose
from ahl.logging import get_logger
from equities.centaur.data import get_stocks_item
from equities.centaur.mid_freq.signal.filings import EventsToTimeSeries

# %matplotlib inline
get_ipython().magic(u'matplotlib inline')
logger = get_logger(__name__)




# + {"tags": ["parameters"]}
MONGOOSE_DB = 'research'
LIBRARY_NAME = 'kepler.FLOW'
DATA_NAME = 'KEPLER_TURNOVER'
DATE_COLUMN = 'DATE'
REGION = 'PANEURO'
FFILL = (22, 66, 130)
UNIQUE_DATES = True


# +
library = Mongoose(MONGOOSE_DB).get_library(LIBRARY_NAME)
data = LIBRARY.read(DATA_NAME).data


# +
def point_in_time_coverage_analysis(df, date_column, region, ffill, unique_dates):
    # date_column must be in dt.date format
    logger.info("Retrieving trading filter")
    tf_ahl = get_stocks_item('swift_trading_filter', region, 'equities.centaur').pd
    tf_ahl.index = tf_ahl.index.to_series().dt.date
    logger.info("Sorting data")
    df_trimmed = df.copy()
    try:
        df_trimmed[date_column] = df_trimmed[date_column].dt.date
    except AttributeError:
        pass
    df_trimmed = df_trimmed[df_trimmed.ahl_id != ''].reset_index().drop_duplicates([date_column, 'ahl_id'])                                    .sort_values(date_column)
    df_trimmed['filter'] = 1
    if unique_dates:
        tf_ahl = tf_ahl.loc[df_trimmed[date_column].unique()].dropna()
    logger.info("Calculating coverage")
    evt2ts = EventsToTimeSeries(field='filter', date_column=date_column)
    tf_data = evt2ts(df_trimmed, tf_ahl)
    res = pd.DataFrame(index=tf_ahl.index)
    # 100 * to turn value into percentage
    res['0 ffill size'] = 100 * tf_data.multiply(tf_ahl, fill_value=0).sum(axis=1) / tf_ahl.sum(axis=1)
    res['Mapped symbols 0 ffill limit'] = tf_data.multiply(tf_ahl, fill_value=0).sum(axis=1)
    res['AHL universe'] = tf_ahl.sum(axis=1)
    for ffill_size in ffill:
        logger.info('{} ffill limit'.format(ffill_size))
        tf_data_ffill = tf_data.ffill(limit=ffill_size)
        tf_data_ffill_adj = tf_data_ffill.multiply(tf_ahl, fill_value=0)
        # 100 * to turn value into percentage
        res['{} ffill limit'.format(ffill_size)] = 100 * tf_data_ffill_adj.sum(axis=1 )/ tf_ahl.sum(axis=1)
        res['Mapped symbols {} ffill limit'.format(ffill_size)] = tf_data_ffill_adj.multiply(tf_ahl, fill_value=ffill_size).sum(axis=1)
    res_trimmed = res.loc[df_trimmed[date_column].min():df_trimmed[date_column].max()]
    if unique_dates:
        res_trimmed = res_trimmed.loc[df_trimmed[date_column].unique()].dropna()
    res_trimmed.index = pd.to_datetime(res_trimmed.index)
    return res_trimmed


def plot_coverage(df):
    ax = df.loc[:,(~df.columns.str.startswith('Mapped')) & (df.columns != 'AHL universe')].plot()
    ax1 = df.loc[:,df.columns.str.startswith('Mapped') | (df.columns == 'AHL universe')].plot()
    ax.grid(False)
    ax1.grid(False)
    ax.set_ylabel('% coverage')
    ax1.set_ylabel('# Companies')
    return(ax, ax1)


# -

coverage_df = point_in_time_coverage_analysis(df=DATA,
                                    date_column=DATE_COLUMN,
                                    region=REGION,
                                    ffill=FFILL,
                                    unique_dates=UNIQUE_DATES)
plot_coverage(coverage_df)
