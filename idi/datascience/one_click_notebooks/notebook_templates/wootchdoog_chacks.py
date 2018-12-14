# ---
# jupyter:
#   celltoolbar: Tags
#   jupytext_format_version: '1.2'
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
library_name = 'cls.CLS_FX_VOLUME'
mongo_host = 'mktdatad'
strategy_name = None
symbols=['SWP:EURUSD', 'SWP:USDJPY', 'SWP:GBPUSD', 'ORF:EURUSD', 'ORF:USDJPY', 'ORF:GBPUSD', 'SPT:EURUSD', 'SPT:USDJPY', 'SPT:GBPUSD']
columns=['volume']

# + {"active": "ipynb", "language": "javascript"}
# IPython.OutputArea.auto_scroll_threshold = 9999;
# -

# %matplotlib inline
from idi.datascience.tools import data_checks

data_checks.check_dataset(library_name,
                          mongo_host=mongo_host,
                          symbols=symbols,
                          cols=columns,
                          plot_data=True,
                          strategy_name=strategy_name)


