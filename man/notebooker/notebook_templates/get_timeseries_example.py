# ---
# jupyter:
#   celltoolbar: Tags
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: spark273
#     language: python
#     name: spark273
# ---

# %matplotlib inline
import ahl.marketdata as amd

# + {"tags": ["parameters"]}
symbol = 'SPT:EURUSD'
library = 'CLS_FX_VOLUME'
columns = ['volume']
# -

amd.get_timeseries(symbol, library=library).pd[columns].plot()
