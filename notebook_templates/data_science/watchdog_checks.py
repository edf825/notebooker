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
library_name = 'haver.HAVER'
mongo_host = 'research'
strategy_name = None
symbols = ['ALPMED:CHNBCGS']
columns = None
max_stdev = 5
max_missing_days = 5
max_stale_days = 5

# + {"active": "ipynb", "language": "javascript"}
# IPython.OutputArea.auto_scroll_threshold = 9999;
# -

# %matplotlib inline
from man.datascience.tools import data_checks

data_checks.check_dataset(library_name,
                          mongo_host=mongo_host,
                          symbols=symbols,
                          cols=columns,
                          plot_data=True,
                          strategy_name=strategy_name,
                          max_stdev=max_stdev,
                          max_missing_days=max_missing_days,
                          max_stale_days=max_stale_days,
                         )


