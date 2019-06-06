# ---
# jupyter:
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: riskdev
#     language: python
#     name: riskdev
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
# %matplotlib inline
from ahl.mongo import Mongoose
import numpy as np
import ahl.marketdata as amd
from datetime import datetime as dt
from ahl.dateutil import DateTimeDelta
import risk.data.utilities as rut
import pandas as pd
from pandas.tseries.offsets import BDay
import matplotlib.pyplot as plt
from logging import getLogger
import re
from datetime import datetime as dt, timedelta

logger = getLogger()

m = Mongoose('research')


amd.enable_trading_mode(False)

UNKNOWN = ''
regex_str_red = '(?<=:)(\d[A-Z]\w{3}[A-Z]{3}\d)' # 4ABCAQAC1?
regex_str_ticker = '([A-Z]+[ .][A-Z]+[.][A-Z]+[.]\d{1,2})'

regex_str_series = '\d+'

# + {"tags": ["parameters"]}
start_date = dt(2019,1,1)

# +
# Load the SDR data (both DTCC and BBG)
dtcc_file = m.get_library('dtcc.SDR')
dtcc_filef = dtcc_file.find({'EXECUTION_TIMESTAMP': {"$gte": start_date}, 'ASSET_CLASS': 'CREDITS'})

dtcc_data = pd.DataFrame(d for d in dtcc_filef)


bbg_file = m.get_library('dtcc.BBG_SDR')
bbg_filef = bbg_file.find({'EXECUTION_TIMESTAMP': {"$gte": start_date}, 'ASSET_CLASS': 'CREDITS'})
bbg_data = pd.DataFrame(b for b in bbg_filef)
# -

# Frame of taxonomy and ticker_stem for AHL CDS markets
mkts = pd.DataFrame(columns=['taxonomy', 'ticker_stem', 'ul_asset_contains', 'bbg_ticker_contains'])
mkts.loc['IA5US', :] = ['CREDIT:INDEX:ITRAXX:ITRAXXAUSTRALIA', 'ITRAXX.AUSTRALIA.', 'ITRAXX AUSTRALIA', 'ITXAA']
mkts.loc['IAX5US', :] = ['CREDIT:INDEX:ITRAXX:ITRAXXASIAEXJAPAN', 'ITRAXX.ASIA.EX.JAPAN.', 'ITRAXX ASIA EX', 'ITXAG']
mkts.loc['I5EU', :] = ['CREDIT:INDEX:ITRAXX:ITRAXXEUROPE', 'ITRAXX.EUROPE.MAIN.', 'ITRAXX EUROPE SERIES', 'ITXEB']
mkts.loc['IEF5EU', :] = ['CREDIT:INDEX:ITRAXX:ITRAXXEUROPE', 'ITRAXX.EUROPE.SNR.FIN.', 'ITRAXX EUROPE SENIOR', 'ITXES']
mkts.loc['IX5EU', :] = ['CREDIT:INDEX:ITRAXX:ITRAXXEUROPE', 'ITRAXX.EUROPE.XOVER.', 'ITRAXX EUROPE CROSSOVER', 'ITXEX']
mkts.loc['I5US', :] = ['CREDIT:INDEX:CDX:CDXIG', 'CDX.NA.IG.', 'CDX.NA.IG', 'CDXIG']
mkts.loc['IEM5US', :] = ['CREDIT:INDEX:CDX:CDXEMERGINGMARKETS', 'CDX.EM.', 'CDX.EM.', 'CXPEM']
mkts.loc['I5JP', :] = ['CREDIT:INDEX:ITRAXX:ITRAXXJAPAN', 'ITRAXX.JAPAN.', 'ITRAXX JAPAN', 'ITXAJ']
mkts.loc['IY5US', :] = ['CREDIT:INDEX:CDX:CDXHY', 'CDX.NA.HY.', 'CDX.NA.HY', 'CXPHY']


# Create mapping from underlying_asset (bbg_ticker) to ticker_stem for dtcc data (bbg data)
dtcc_ticker_map = mkts.loc[:, ['ticker_stem', 'ul_asset_contains']].set_index('ul_asset_contains').squeeze()
dtcc_ticker_map['Other'] = ''
bbg_ticker_map = mkts.loc[:, ['ticker_stem', 'bbg_ticker_contains']].set_index('bbg_ticker_contains').squeeze()
bbg_ticker_map['Other'] = ''

def create_dtcc_ticker(underlying_asset):

    ticker = dtcc_ticker_map.get(next((x for x in dtcc_ticker_map.index.values if x.lower() in underlying_asset.lower()), 'Other'))
    if ticker != '':
        ticker += re.search(regex_str_series, underlying_asset).group().lstrip('0')

    return ticker

def create_bbg_ticker(bbg_ticker):

    ticker = bbg_ticker_map.get(next((x for x in bbg_ticker_map.index.values if x.lower() in bbg_ticker.lower()), 'Other'))
    if ticker != '':
        ticker += bbg_ticker[-2:].lstrip('0')

    return ticker

def list_taxonomies(library='jmao.SDR', asset_class='CREDITS', contains=''):
    file = m.get_library(library)
    data = file.find({'ASSET_CLASS': 'CREDITS'})
    df = pd.DataFrame(d for d in data)

    return [x for x in df.TAXONOMY.unique() if contains in x]

def get_ticker_stems(library='jmao.SDR'):
    file = m.get_library(library)
    data = file.find({'ASSET_CLASS': 'CREDITS'})
    df = pd.DataFrame(d for d in data)
    df.loc[:,'ticker'] = df.UNDERLYING_ASSET_1.str.extract(regex_str_ticker).fillna(UNKNOWN).str.replace(' ','.')
    df.loc[:,'ticker_stem'] = df.ticker.apply(lambda x: ".".join(x.split('.')[:-1]))
    return df.ticker_stem.unique()


def tidy_up_processed_sdr_data(data, bbg_data, taxonomy, ticker_stem=None, from_date=None, to_date=None):

    df = data
    df = df.loc[df.TAXONOMY==taxonomy]
    logger.info('Creating DTCC ticker column')
    df.loc[:,'ticker'] = df.UNDERLYING_ASSET_1.apply(create_dtcc_ticker)
    df.loc[:,'red_id'] = df.UNDERLYING_ASSET_1.str.extract(regex_str_red).fillna(UNKNOWN)

    bbg_df = bbg_data
    if (taxonomy!='CREDIT:INDEX:ITRAXX:ITRAXXJAPAN') & (not bbg_data.empty):
        bbg_df = bbg_df.loc[bbg_df.TAXONOMY==taxonomy]
        bbg_df['FILE_NAME'] = 'BBG'
        logger.info('Creating BBG ticker column')
        bbg_df.loc[:, 'ticker'] = bbg_df.TICKER.apply(create_bbg_ticker)
        bbg_df.loc[:, 'red_id'] = ''
        df = pd.concat(objs=[df, bbg_df], axis=0)

    logger.info('Filling missing tickers')
    red_id_ticker_map = df.loc[~((df[['ticker','red_id']] =='').any(axis=1)) & (df.ticker.str.startswith(ticker_stem)), :].groupby(['red_id'])['ticker'].first().sort_values()
    def missing_ticker_mapper(red_id):
        try:
            return red_id_ticker_map[red_id]
        except KeyError:
            return ''

    df['filled_ticker'] = df.apply(lambda row: missing_ticker_mapper(row.red_id) if row.ticker=='' else row.ticker, axis=1)

    df.loc[:,'expiry_month'] = df.END_DATE.apply(lambda x:"{:%Y%m}".format(x))

    size = df.size
    if ticker_stem:
        logger.info('Removing unwanted tickers')
        df = df.loc[df.filled_ticker.str.startswith(ticker_stem)]
        logger.info('{} trades removed'.format(size - df.size))
        size = df.size
    logger.info('Removing unwanted trades...')
    if from_date:
        logger.info('Removing trades before from_date')
        df = df.loc[df.EXECUTION_TIMESTAMP > from_date]
        logger.info('{} trades removed'.format(size - df.size))
        size = df.size
    if to_date:
        logger.info('Removing trades before from_date')
        df = df.loc[df.EXECUTION_TIMESTAMP < to_date]
        logger.info('{} trades removed'.format(size - df.size))
        size = df.size
    logger.info('Removing trades with embeded options')
    if 'EMBEDED OPTION' in df.columns:
        mask_exclude_embedded_opt = ~df.loc[:,'EMBEDED_OPTION'].isin(['EMBED1'])
        df = df[mask_exclude_embedded_opt]
    logger.info('{} trades removed'.format(size - df.size))
    size = df.size
    logger.info('Removing non-trades')
    if 'PRICE_FORMING_CONTINUATION_DATA' in df.columns:
        mask_only_trades = df.loc[:,'PRICE_FORMING_CONTINUATION_DATA'] == 'Trade'
        df = df[mask_only_trades]
    logger.info('{} trades removed'.format(size - df.size))
    size = df.size
    logger.info('Removing trades with zero or missing notional')
    mask_remove_zero_ntl = df.loc[:,'ROUNDED_NOTIONAL_AMOUNT_1'].fillna(0)!=0
    df = df[mask_remove_zero_ntl]
    logger.info('{} trades removed'.format(size - df.size))
    size = df.size
    logger.info('Removing trades for which a ticker or red_id could not be mapped')
    mask_unmapped = (df.red_id!='') |(df.ticker!='')
    df = df[mask_unmapped]
    logger.info('{} trades removed'.format(size - df.size))
    size = df.size
    logger.info('Removing trades with PRICE_NOTATION_TYPE == AMOUNT')
    df = df.loc[df.PRICE_NOTATION_TYPE != 'Amount']
    size = df.size
    logger.info('Removing trades with missing price')
    df = df.loc[df.PRICE_NOTATION!='']
    logger.info('{} trades removed'.format(size - df.size))
    size = df.size
    logger.info('Removing trades with missing notional')
    df = df.loc[df.ROUNDED_NOTIONAL_AMOUNT_1!='']
    logger.info('{} trades removed'.format(size - df.size))
    size = df.size

    logger.info('Removing unwanted columns')
    #df.drop(columns=['INDICATION_OF_COLLATERALIZATION', 'DAY_COUNT_CONVENTION', 'ASSET_CLASS', 'PRICE_FORMING_CONTINUATION_DATA',
    #                 'INDICATION_OF_OTHER_PRICE_AFFECTING_TERM', 'NOTIONAL_CURRENCY_1', 'PAYMENT_FREQUENCY_1', 'INDICATION_OF_END_USER_EXCEPTION',
    #                 'TAXONOMY_TAIL', 'SUB-ASSET_CLASS_FOR_OTHER_COMMODITY', 'UNDERLYING_ASSET_2', 'PRICE_NOTATION3', 'PRICE_NOTATION3_TYPE',
    #                 'ROUNDED_NOTIONAL_AMOUNT_2_LB', 'PRICE_NOTATION2_TYPE', 'PRICE_NOTATION2', 'OPTION_PREMIUM', 'OPTION_LOCK_PERIOD',
    #                 'OPTION_EXPIRATION_DATE', 'OPTION_TYPE', 'OPTION_FAMILY', 'OPTION_CURRENCY', 'RESET_FREQUENCY_2', 'NOTIONAL_CURRENCY_2',
    #                 'PAYMENT_FREQUENCY_2', 'EMBEDED_OPTION', 'OPTION_STRIKE_PRICE', 'ROUNDED_NOTIONAL_AMOUNT_2', 'RESET_FREQUENCY_1'], inplace=True)

    columns_to_keep = ['EXECUTION_TIMESTAMP', 'CLEARED', 'BLOCK_TRADES_AND_LARGE_NOTIONAL_OFF-FACILITY_SWAPS', 'INDICATION_OF_COLLATERALIZATION', 'EXECUTION_VENUE',
                       'EFFECTIVE_DATE', 'END_DATE', 'SETTLEMENT_CURRENCY', 'UNDERLYING_ASSET_1', 'PRICE_NOTATION_TYPE', 'PRICE_NOTATION',
                       'ROUNDED_NOTIONAL_AMOUNT_1', 'FILE_NAME', 'expiry_month', 'red_id', 'filled_ticker']
    df = df[columns_to_keep]

    logger.info('Adding maturity column')
    maturity_map = df.groupby(['filled_ticker', 'END_DATE'])['EFFECTIVE_DATE'].quantile(0.05).sort_index(level=0).reset_index(level=1)
    maturity_map['maturity'] = ((maturity_map.END_DATE - maturity_map.EFFECTIVE_DATE).dt.days / 365.0).round(0).astype(int)
    maturity_map = maturity_map.groupby(['filled_ticker', 'END_DATE']).sum()

    def maturity_mapper(ticker, end_date):
        idx = pd.IndexSlice
        try:
            return maturity_map.loc[idx[ticker, end_date], 'maturity'].squeeze()
        except KeyError:
            pass
        except IndexError:
            pass

    df['maturity'] = df.apply(lambda row: maturity_mapper(row.filled_ticker, row.END_DATE), axis=1)

    def is_otr(ticker, execution_date, s10_start_date=dt(2008,9,20)):


        if 'CDX.NA' in ticker:
            s10_start_date = dt(2008,3,21)
        series = int(ticker.split(".")[-1])
        roll_date = s10_start_date + DateTimeDelta(months = 6 * (series-9))
        roll_date = rut.get_next_gbd(roll_date.replace(day=19))
        otr = execution_date < roll_date
        return otr
    logger.info('Adding OTR column')
    df['OTR'] = df.apply(lambda row: is_otr(row.filled_ticker, row.EXECUTION_TIMESTAMP), axis=1)

    return df


def adv(df, rolling_window=20, maturity=5.0):
    grouped_df = df.copy()
    grouped_df.EXECUTION_TIMESTAMP = grouped_df.EXECUTION_TIMESTAMP.dt.date
    grouped_df = grouped_df.groupby(['EXECUTION_TIMESTAMP', 'maturity'])['ROUNDED_NOTIONAL_AMOUNT_1'].sum()
    rolling_median = grouped_df.unstack(1).fillna(method='ffill', limit=5).rolling(rolling_window).median().loc[:, maturity].to_frame() / 1e6
    return rolling_median

def avg_price(df, rolling_window=20, maturity=5.0):
    grouped_df = df.copy()
    grouped_df.EXECUTION_TIMESTAMP = grouped_df.EXECUTION_TIMESTAMP.dt.date
    grouped_df.set_index('EXECUTION_TIMESTAMP', inplace=True)
    daily_avg_price = grouped_df.groupby('EXECUTION_TIMESTAMP')['PRICE_NOTATION'].quantile(0.5)
    grp = grouped_df.groupby('EXECUTION_TIMESTAMP')['ROUNDED_NOTIONAL_AMOUNT_1'].sum()
    grouped_df['total_daily_ntl'] = grp
    grouped_df['avg_daily_price'] = daily_avg_price
    grouped_df = grouped_df.loc[grouped_df.PRICE_NOTATION < 3 * grouped_df.avg_daily_price]
    grouped_df['pct_of_daily_ntl'] = grouped_df.ROUNDED_NOTIONAL_AMOUNT_1 / grouped_df.total_daily_ntl
    grouped_df['ntl_weighted_price'] = grouped_df['pct_of_daily_ntl'] * grouped_df['PRICE_NOTATION']

    return grouped_df.groupby('EXECUTION_TIMESTAMP')['ntl_weighted_price'].sum()


# +
def summary_stats(df, stem, start_date=None, end_date=None, otr_filter=False):
    if start_date:
        df = df.loc[df.EXECUTION_TIMESTAMP > start_date]
    if end_date:
        df = df.loc[df.EXECUTION_TIMESTAMP < end_date]
    if otr_filter:
        df = df.loc[df.OTR==True]

    stats = pd.DataFrame(columns=['Yes', 'No'])

    stats.loc['Cleared', 'Yes'] = df.loc[df.CLEARED=='C'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['Cleared', 'No'] = df.loc[df.CLEARED=='U'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    stats.loc['SEF_Executed', 'Yes'] = df.loc[df.EXECUTION_VENUE.isin(['ON', 'OFF'])].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['SEF_Executed', 'No'] = df.loc[df.EXECUTION_VENUE=='OFF'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    # https://www.cftc.gov/sites/default/files/idc/groups/public/@newsroom/documents/file/block_qa_final.pdf
    stats.loc['Block_Trades', 'Yes'] = df.loc[df['BLOCK_TRADES_AND_LARGE_NOTIONAL_OFF-FACILITY_SWAPS']=='Y'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['Block_Trades', 'No'] = df.loc[df['BLOCK_TRADES_AND_LARGE_NOTIONAL_OFF-FACILITY_SWAPS']=='N'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    stats.loc['Collateralized', 'Yes'] = df.loc[(df.CLEARED=='U') & (df.INDICATION_OF_COLLATERALIZATION.isin(['PC', 'FC', 'OC']))].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.loc[df.CLEARED=='U'].ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['Collateralized', 'No'] = df.loc[(df.CLEARED=='U') & (df.INDICATION_OF_COLLATERALIZATION=='UC')].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.loc[df.CLEARED=='U'].ROUNDED_NOTIONAL_AMOUNT_1.sum()

    stats.loc['10y_maturity', 'Yes'] = df.loc[df.maturity==10].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['5y_maturity', 'Yes'] = df.loc[df.maturity==5].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['other_maturity', 'Yes'] = df.loc[~df.maturity.isin([5, 10])].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    stats.loc['OTR', 'Yes'] = df.loc[df.OTR==True].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['OTR', 'No'] = df.loc[df.OTR==False].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    #ax1.set_ylabel('')
    #stats.loc['Cleared', :].plot(kind='pie', ax=ax1, autopct='%1.1f%%', startangle=45, shadow=False, labels=['Cleared', 'Uncleared'], legend=False, fontsize=14, title='Cleared vs. Uncleared')

    fig, axarr = plt.subplots(3,2, figsize=(10,20))
    fig.suptitle('{} Summary Stats'.format(stem), fontname='Times New Roman', fontweight='bold', size=20, va='bottom')

    axarr[0, 0].pie(stats.loc['SEF_Executed', :], explode=(0, 0), labels=['SEF', 'Non-SEF'], autopct='%1.1f%%', shadow=False, startangle=45)
    axarr[0, 0].set_title(('Traded via SEF'))

    axarr[0, 1].pie(stats.loc['Cleared', :], explode=(0, 0), labels=['Cleared', 'Uncleared'], autopct='%1.1f%%', shadow=False, startangle=45)
    axarr[0, 1].set_title('Cleared vs. Uncleared')

    axarr[1, 0].pie(stats.loc['Block_Trades', :], explode=(0, 0), labels=['Block Trade', 'Non-Block Trade'], autopct='%1.1f%%', shadow=False, startangle=45)
    axarr[1, 0].set_title('Block trades (Note - uses capped notional)')

    axarr[1, 1].pie(stats.loc[['5y_maturity', '10y_maturity', 'other_maturity'], 'Yes'], labels=['5y', '10y', 'other'], autopct='%1.1f%%', shadow=False, startangle=45)
    axarr[1, 1].set_title('Trade Maturities')

    axarr[2, 0].pie(stats.loc['OTR', :], explode=(0, 0), labels=['On the run', 'Off the run'], autopct='%1.1f%%', shadow=False, startangle=45)
    axarr[2, 0].set_title('On the run vs. Off the run')

    trends_stats = summary_stats_trends(df)
    trends_stats.plot(kind='bar', ax=axarr[2, 1], title='Trend of Summary Stats')

    return stats




# -

def summary_stats_trends(df, otr_filter=False):

    #plt.suptitle()

    stats = pd.DataFrame(columns=['Last Month', '1 Month Ago'])

    most_recent_date = df.EXECUTION_TIMESTAMP.dt.date.max()
    one_month_ago = most_recent_date - timedelta(30)
    two_months_ago = one_month_ago - timedelta(30)

    df_recent = df.loc[df.EXECUTION_TIMESTAMP>one_month_ago]
    df_1m = df.loc[(df.EXECUTION_TIMESTAMP>two_months_ago) & (df.EXECUTION_TIMESTAMP<one_month_ago)]

    stats.loc['Cleared (%)', 'Last Month'] = df_recent.loc[df_recent.CLEARED=='C'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_recent.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['Cleared (%)', '1 Month Ago'] = df_1m.loc[df_1m.CLEARED=='C'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_1m.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    stats.loc['SEF Executed (%)', 'Last Month'] = df_recent.loc[df_recent.EXECUTION_VENUE.isin(['ON', 'OFF'])].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_recent.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['SEF Executed (%)', '1 Month Ago'] = df_1m.loc[df_1m.EXECUTION_VENUE.isin(['ON', 'OFF'])].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_1m.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    stats.loc['Block Trades (%)', 'Last Month'] = df_recent.loc[df_recent['BLOCK_TRADES_AND_LARGE_NOTIONAL_OFF-FACILITY_SWAPS']=='Y'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_recent.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['Block Trades (%)', '1 Month Ago'] = df_1m.loc[df_1m['BLOCK_TRADES_AND_LARGE_NOTIONAL_OFF-FACILITY_SWAPS']=='Y'].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_1m.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    stats.loc['5y Maturity (%)', 'Last Month'] = df_recent.loc[df_recent.maturity==5].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_recent.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['5y Maturity (%)', '1 Month Ago'] = df_1m.loc[df_1m.maturity==5].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_1m.ROUNDED_NOTIONAL_AMOUNT_1.sum()

    stats.loc['OTR (%)', 'Last Month'] = df_recent.loc[df_recent.OTR==True].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_recent.ROUNDED_NOTIONAL_AMOUNT_1.sum()
    stats.loc['OTR (%)', '1 Month Ago'] = df_1m.loc[df_1m.OTR==True].ROUNDED_NOTIONAL_AMOUNT_1.sum() * 100 / df_1m.ROUNDED_NOTIONAL_AMOUNT_1.sum()


    return stats

# +
for x in mkts.values:
    res = tidy_up_processed_sdr_data(dtcc_data, bbg_data, x[0], x[1], start_date)
    summary_stats(res, x[1])
    fig1, ax1 = plt.subplots(figsize=(20,10))
    adv(res).plot(title=x[1]+'-VOLUME (millions)', ax=ax1)
    fig2, ax2 = plt.subplots(figsize=(20,10))
    avg_price(res).plot(title=x[1]+'-NOTIONAL WEIGHTED AVG PRICE', ax=ax2)
