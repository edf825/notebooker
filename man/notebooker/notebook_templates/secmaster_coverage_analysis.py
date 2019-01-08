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

# + {"tags": ["parameters"]}
cluster = 'research'  # cluster for the security master e.g. research/mktdatad
region = 'US'  # Trading filter region used by the equities.centaur.data.get_stocks_item
trading_filter_type = 'swift_trading_filter'  # Trading filter type to be used by the get_stocks_item
date_start = None  # Optionally set this to a date from which you want to make the plot
target_identifier = 'DXLID'  # This is the target identifier. e.g. DXLID for mappings from AST to DXLID
# -

# %matplotlib inline
import datetime
from collections import defaultdict

import matplotlib.pyplot as plt
import pandas as pd
import pytz
from intervaltree import IntervalTree
from tqdm import tqdm
from typing import List, Optional, AnyStr, DefaultDict

from equities.centaur.data import get_stocks_item
from man.security_master.api import SecurityMasterAPI
from man.security_master.sources.base import IdentifierType, Route


def get_trading_filter(region, filter_type='swift_trading_filter'):
    # type: (AnyStr, Optional[AnyStr]) -> pd.DataFrame
    """Returns filter specifying dates on which ASTs were investable. Omits non investable ASTs.
    filter_type can be 'download_filter' (MSCI IMI) or 'swift_trading_filter' (Swift investable)"""
    trading_filter = get_stocks_item(filter_type, region).pd
    all_asts = trading_filter.loc[:, (trading_filter != 0).any(axis=0)]
    return all_asts


def lookup_secmaster_for_ast(sec_master_api, routes, ast):
    # type: (SecurityMasterAPI, List[Route], AnyStr) -> IntervalTree
    to_return = []
    for route in routes[:1]:
        digraph = sec_master_api.query(
            starting_identifier=ast,
            starting_identifier_type=IdentifierType.AHL_EQUITY_ID,
            route=route
        )
        to_return.append(digraph.get_intervaltree_for_identifier(IdentifierType.DXLID))
    return IntervalTree({ints for intervaltree in to_return for ints in intervaltree})


title = u'Coverage Analysis {} | AST -> {}'.format(region, target_identifier)

# Equities Centaur
trading_filter = get_trading_filter(region)  # This is the only source for the trading filter as of now...
all_symbols_ever = list(trading_filter.columns)

# Security Master
sec_master_api = SecurityMasterAPI.from_cluster(cluster)
routes = sec_master_api.routes_between_types(IdentifierType.AHL_EQUITY_ID, IdentifierType(target_identifier))

symbol_intervaltrees = {}
for ast in tqdm(all_symbols_ever):
    symbol_intervaltrees[ast] = lookup_secmaster_for_ast(sec_master_api, routes, ast)

n_eq_mapped = defaultdict(int)          # type: DefaultDict[datetime.datetime, int]
n_symbols_mapped = defaultdict(int)     # type: DefaultDict[datetime.datetime, int]
n_total_equities = defaultdict(int)     # type: DefaultDict[datetime.datetime, int]
for day, row in tqdm(trading_filter.iterrows()):
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

    print('{}: {}/{} equities were mapped to {} symbols'.format(day, n_eq_mapped[day], n_total_equities[day],
                                                                n_symbols_mapped[day]))
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
