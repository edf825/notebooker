# ---
# jupyter:
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: counterparty
#     language: python
#     name: counterparty
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

import pandas as pd
from datetime import datetime as dt
import risk.data.utilities as rut
from risk.data.counterparty import _get_product_cpty_table, _get_missing_cparty_spreads
from risk.data.counterparty import *

# + {"tags": ["parameters"]}
counterparty = ['Deutsche Bank AG', 'DEUTSCHE BANK AG -REG']
# -

FUNDID = 'FundId'
UNIT = 'Unit'
run_date = rut.day_begin(rut.get_last_gbd(dt.now()))

report_id = 75517
pam_res, _ = rut.PAM_PROXY.execute_report(report_id=report_id, start_date=run_date, end_date=run_date)

# +
use_subnames=True
lookthrough=False
ucits_filter=False
drop_asset_types=None
include_ccp=False
end_dt = run_date

counterparty_name_field = 'AHLCounterpartySubName' if use_subnames else AHL_CPTY_NAME
KEEP_COLS = OrderedDict([(rdpn.PERIOD_END, DATE),
                         ('FUNDID', FUND),
                         ('LegalEntityFundInvestmentManager', 'Unit'),
                         ('FundId','FundId'),
                         (counterparty_name_field, CPTY),
                         (CCP_NAME, CCP_NAME),
                         (ACC_TYPE, ACC_TYPE),
                         (WRST_RTG, RTG),
                         (SPREAD_FLD, CREDIT_SPRD),
                         (ACT_CLASS, ACT_CLASS),
                         (USD_BAL, USD_BAL),
                        ])
                         # ('FundCloseCapital', 'FundCloseCapital'),])
raw_data = pam_res.copy()
raw_data.loc[raw_data['LegalEntityFundInvestmentManager'].isnull(), 'LegalEntityFundInvestmentManager'] = 'UNKNOWN'
raw_data.loc[raw_data['FundMnemonic']=='MFD2','LegalEntityFundInvestmentManager']='AHL Partners LLP'
# raw_data.loc[raw_data['FundId']==10035, 'LegalEntityFundInvestmentManager']='FRM Investment Management Limited'

if not include_ccp:
    KEEP_COLS.pop(CCP_NAME)
    KEEP_COLS.pop(ACC_TYPE)
mask = raw_data[AHL_CPTY_NAME] == INTER_FUND_CLASS_LOAN
raw_data.loc[mask, WRST_RTG] = None
raw_data.loc[mask, SPREAD_FLD] = 0.
raw_data.loc[mask, ACT_ID] = None  # this prevents the credit spread being looked up further down

filtered_data = raw_data.loc[(raw_data.loc[:, USD_BAL] != 0.), :]

# exclude certain asset types - can be useful when running without lookthrough
if drop_asset_types is not None:
    filtered_data = filtered_data.loc[~raw_data.loc[:, 'SecurityClass'].isin(drop_asset_types)]

# Govvie spreads aren't consistent even for same issuer, so blank them out (as can cause issues with grouping)
filtered_data.loc[filtered_data.loc[:, TBILL_FLAG], SPREAD_FLD] = 0.

if not lookthrough:
    # Fund Linked note covers MAT0, Fund TRS (Funded) covers insti set-up
    glg_mask = filtered_data.SecurityClass.isin(['GLG Fund CIS', 'Fund Linked Note', 'Fund TRS (Funded)'])
    filtered_data.loc[glg_mask, 'AccountType'] = 'FundHolding'
    filtered_data.loc[glg_mask, 'AccountClass'] = 'fund'
    filtered_data.loc[glg_mask, counterparty_name_field] = 'Man Group Fund'
    filtered_data.loc[glg_mask, SPREAD_FLD] = 0.
    filtered_data.loc[glg_mask, WRST_RTG] = None
    filtered_data.loc[glg_mask, ACT_ID] = None

# If looking on a UCITS basis,we exclude cash and cash equivalents, and prime broker accounts, as per 20602
if ucits_filter:
    filtered_data = filtered_data.loc[
        (filtered_data.UCITSAcctType != 'Deposits') & (filtered_data.AccountType != 'Prime Broker')]

# Data is missing when the report runs live
if end_dt is None:
    end_dt = raw_data.loc[:, rdpn.PERIOD_END].max().to_datetime()
if end_dt >= rut.day_begin(dt.now()):
    today_rows = filtered_data.loc[:, rdpn.PERIOD_END] == rut.day_begin(end_dt)
    filtered_data.loc[today_rows, [SPREAD_FLD]] = \
        filtered_data.loc[today_rows, :].apply(
            lambda row: _get_missing_cparty_spreads(CREDIT_SPRD, product, row), axis=1)
    filtered_data.loc[today_rows, [AHL_CPTY_NAME]] = \
        filtered_data.loc[today_rows, :].apply(
            lambda row: _get_missing_cparty_spreads(AHL_CPTY_NAME, product, row), axis=1)

filtered_data = filtered_data.loc[:, KEEP_COLS.keys()]

filtered_data.rename(columns=KEEP_COLS, inplace=True)
# fill remaining nas with "", as otherwise fails in pivot, deal with if breaks down in formatting
filtered_data.loc[:, KEEP_COLS.values()[2:-1]] = filtered_data.loc[:, KEEP_COLS.values()[2:-1]].fillna("")
pivot = filtered_data.groupby(KEEP_COLS.values()[:-1]).sum().unstack()
pivot.columns = pivot.columns.droplevel(0)
pivot.columns.name = ""
missing_clabels = set(pivot.columns) - set(CPTY_CLABELS_MAP)
assert missing_clabels == set(), "CPTY_CLABELS_MAP is missing '{}'".format("','".join(missing_clabels))
pivot = pivot.loc[:, CPTY_CLABELS_MAP].dropna(how='all', axis=1)
pivot.loc[:, CPTY_TOTAL_CLABEL] = pivot.sum(axis=1)

# +
ctpy_data_raw = pivot.groupby(level=[DATE,UNIT,FUND,FUNDID, CPTY, RTG, CREDIT_SPRD]).sum()
ctpy_data = ctpy_data_raw.loc[pd.IndexSlice[:,:,:,:,counterparty,:,:],].loc[run_date]
ctpy_data /= 1e6
ctpy_data.rename(columns=CPTY_CLABELS_MAP, inplace=True)
ctpy_data.columns = ['{}m'.format(u) for u in ctpy_data.columns]
ctpy_data.reset_index([FUNDID, CPTY, RTG, CREDIT_SPRD], inplace=True)
# keep_cols = ['Counterparty', 'Rating', 'CreditSpread', 'cash', 'depo', 'OTC Margin, $m']
# ctpy_data = ctpy_data.loc[:,keep_cols]

ctpy_data_by_unit = ctpy_data.copy()


ctpy_data = ctpy_data.loc[ctpy_data['Total Exp, $m']>0.1]
ctpy_data = ctpy_data.round(1)

# -

ctpy_data_by_unit.reset_index().groupby([UNIT, CPTY]).sum().sort_values('Total Exp, $m', ascending=False).round(1).drop(FUNDID, axis=1)

ctpy_data.sort_values('Total Exp, $m', ascending=False).sort_index(level='Unit', sort_remaining=False)
