# ---
# jupyter:
#   celltoolbar: Tags
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: spark273
#     language: python
#     name: spark273
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

# %matplotlib inline
import ahl.marketdata as amd
from ahl.mongo import MONGOOSE_DB

# + {"tags": ["parameters"]}
symbols = ['SPT:EURUSD', 'SWP:EURUSD', 'ORF:EURUSD', 'SPT:USDCAD', 'SWP:USDCAD', ]
library = 'CLS_FX_VOLUME'
columns = ['volume']
mongo_host = 'research'
# -

amd.enable_trading_mode(False)
MONGOOSE_DB.db_connect(mongo_host)
for symbol in symbols:
    amd.get_timeseries(symbol, library=library).pd[columns].plot()
