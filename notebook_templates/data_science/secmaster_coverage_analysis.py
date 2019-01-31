# ---
# jupyter:
#   celltoolbar: Tags
#   jupytext_format_version: '1.3'
#   kernelspec:
#     display_name: check_kernel
#     language: python
#     name: check_kernel
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

import datetime

# + {"tags": ["parameters"]}
cluster = 'research'  # cluster for the security master e.g. research/mktdatad
region = 'US'  # Trading filter region used by the equities.centaur.data.get_stocks_item
trading_filter_type = 'swift_trading_filter'  # Trading filter type to be used by the get_stocks_item
date_start = datetime.datetime(2016, 1, 1)  # Optionally set this to a date from which you want to make the plot
target_identifier = 'DXLID'  # This is the target identifier. e.g. DXLID for mappings from AST to DXLID
# -

# %matplotlib inline
from collections import defaultdict

import matplotlib.pyplot as plt
import pandas as pd
import pytz
from ahl.logging import logger
from intervaltree import IntervalTree
from typing import List, Optional, AnyStr, DefaultDict

from equities.centaur.data import get_stocks_item
from man.security_master import base
from man.security_master.api import SecurityMasterAPI


def get_trading_filter(region, filter_type='swift_trading_filter', start_date=None):
    # type: (AnyStr, Optional[AnyStr], Optional[datetime.datetime]) -> pd.DataFrame
    """Returns filter specifying dates on which ASTs were investable. Omits non investable ASTs.
    filter_type can be 'download_filter' (MSCI IMI) or 'swift_trading_filter' (Swift investable)"""
    trading_filter = get_stocks_item(filter_type, region).pd
    if start_date:
        trading_filter = trading_filter[start_date:]
    all_active = trading_filter.loc[:, (trading_filter != 0).any(axis=0)]
    return all_active


def lookup_secmaster_for_ast(sec_master_api, routes, ast):
    # type: (SecurityMasterAPI, List[base.Route], AnyStr) -> IntervalTree
    to_return = []
    for route in routes[:1]:
        digraph = sec_master_api.query(
            starting_identifier=ast,
            starting_identifier_type=base.IdentifierType.AHL_EQUITY_ID,
            route=route
        )
        to_return.append(digraph.get_intervaltree_for_identifier(base.IdentifierType.DXLID))
    it = IntervalTree({ints for intervaltree in to_return for ints in intervaltree})
    return it


title = u'Coverage Analysis {} | AST -> {}'.format(region, target_identifier)

# Equities Centaur
trading_filter = get_trading_filter(region,
                                    filter_type=trading_filter_type,
                                    start_date=date_start)  # This is the only source for the trading filter as of now.
all_symbols_ever = list(trading_filter.columns)

# Security Master
sec_master_api = SecurityMasterAPI.from_cluster(cluster)
routes = sec_master_api.routes_between_types(base.IdentifierType.AHL_EQUITY_ID, base.IdentifierType(target_identifier))

symbol_intervaltrees = {}
for i, ast in enumerate(all_symbols_ever):
    if i % 250 == 0:
        logger.info('%s/%s ASTs mapped using secmaster', i, len(all_symbols_ever))
    symbol_intervaltrees[ast] = lookup_secmaster_for_ast(sec_master_api, routes, ast)
logger.info('AST Mapping finished.')

n_eq_mapped = defaultdict(int)          # type: DefaultDict[datetime.datetime, int]
n_symbols_mapped = defaultdict(int)     # type: DefaultDict[datetime.datetime, int]
n_total_equities = defaultdict(int)     # type: DefaultDict[datetime.datetime, int]
for i, (day, row) in enumerate(trading_filter.iterrows()):
    all_asts = list(row.loc[row == 1].index)
    if date_start and day < date_start:
        continue
    day_utc = pytz.utc.localize(day)
    for ast in all_asts:
        intervaltree = symbol_intervaltrees.get(ast)
        n_total_equities[day] += 1
        if not intervaltree:
            continue
        mapped = intervaltree[day_utc]
        if mapped:
            n_eq_mapped[day] += 1
            n_symbols_mapped[day] += len(mapped)
    if i % 250 == 0:
        logger.info('%s/%s days\' coverage calculated', i, len(trading_filter))
        logger.info('{}: {}/{} equities were mapped to {} symbols'.format(
            day, n_eq_mapped[day], n_total_equities[day], n_symbols_mapped[day]))
logger.info('All days finished calculating.')

rows = [
    {'day': day,
     'mapped_symbols': n_symbols_mapped[day],
     'coverage': (n_eq_mapped[day] * 1.0) / n_total_equities[day] if n_total_equities[day] else 0.}
    for day
    in n_eq_mapped]
out_dataframe = pd.DataFrame(rows).set_index('day')
out_dataframe['coverage'].plot(title=title + ' | Coverage over Time', legend=True)
plt.show()
out_dataframe['mapped_symbols'].plot(title=title + ' | # of mappings over Time', legend=True)
plt.show()
