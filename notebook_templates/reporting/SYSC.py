# ---
# jupyter:
#   celltoolbar: Tags
#   jupytext_format_version: '1.2'
#   kernelspec:
#     display_name: my.notebooks
#     language: python
#     name: my.notebooks
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
import numpy as np
import requests
import json
import urllib
from requests.auth import HTTPBasicAuth
from datetime import date, datetime, timedelta
from IPython.display import Markdown as md


# + {"tags": ["parameters"]}
month = "last"

# +
if month == "last":
    month = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
    
md("# Sysc KRI Report for " + month)

# +
start = datetime.strptime(month, "%Y-%m-%d").date()
end = date(start.year, (start.month % 12) + 1, start.day)

days = pd.date_range(start=start, end=end).tolist()

# +
start_day = None
query_by_day = list()

base_query = "assignment_group=7aff6dd6d952120064ca4057f278576b^start_date>={}^start_date<{}^state=7"
for day in days:
    if start_day:
        day_query = base_query.format(start_day.strftime('%Y-%m-%d'), day.strftime('%Y-%m-%d'))
        query_by_day.append(urllib.quote_plus(day_query))
        
    start_day = day
    
month_data = list()

for query in query_by_day:
    url = "http://cr-tool.svc.res.ahl/api/live/query/change_request/{}".format(query)
    cr_response = requests.get(url)
    if cr_response.status_code == 200:
        json_data = json.loads(cr_response.text)
        month_data.extend(json_data)
# -

# # All Changes in last month

df = pd.DataFrame.from_records(month_data, columns=['number', 'short_description', 'u_environment', 'u_completion_code', 'start_date', 'requested_by'])
df['start_date'] = pd.to_datetime(df['start_date'])
df

# # Problem Changes in last month

last_month = df
problems = last_month[last_month['u_completion_code'] != 'Closed complete']
problems

# # Release KRIs

# +
# Split by env
all_changes = last_month.shape[0]
live_all = last_month[last_month['u_environment'] == 'production'].shape[0]
live_problems = problems[problems['u_environment'] == 'production'].shape[0]
live_percent = int((float(live_problems) / live_all) * 100)
staging_all = last_month[last_month['u_environment'] == 'staging'].shape[0]
staging_problems = problems[problems['u_environment'] == 'staging'].shape[0]
staging_percent = int((float(staging_problems) / staging_all) * 100)

(live_all, live_problems, live_percent, staging_all, staging_problems, staging_percent)

data = [
    ['All changes', all_changes],
    ['Live All', live_all],
    ['Live Problems', live_problems],
    ['Live Percent', live_percent],
    ['Staging All', staging_all],
    ['Staging Problems', staging_problems],
    ['Staging Percent', staging_percent]]

release_kri = pd.DataFrame(data, columns=['Metric', 'Measure'])
release_kri
# -

# # Workload KRIs

# +
noon_gap_changes = last_month[last_month['start_date'].dt.strftime('%H:%M:%S').between('12:00:00', '12:30:00')]
max_in_noon = int(noon_gap_changes.groupby(pd.Grouper(key='start_date', freq='1d')).count()['u_environment'].squeeze().max())
avg_in_noon = noon_gap_changes.groupby(pd.Grouper(key='start_date', freq='1d')).count()['u_environment'].squeeze()
avg_in_noon = avg_in_noon[avg_in_noon > 0].mean()


data = [
    ['Max in Noon', max_in_noon],
    ['Avg in Noon', avg_in_noon]
]

workload_kri = pd.DataFrame(data, columns=['Metric', 'Measure']).round({'Measure': 2})
workload_kri
# -

# # High and Critical Incidents

# +
start_day = None
query_by_day = list()

base_query = "assignment_group=7aff6dd6d952120064ca4057f278576b^opened_at>={}^opened_at<{}^priority>=3"
for day in days:
    if start_day:
        day_query = base_query.format(start_day.strftime('%Y-%m-%d'), day.strftime('%Y-%m-%d'))
        query_by_day.append(urllib.quote_plus(day_query))
        
    start_day = day
    
month_data = list()

for query in query_by_day:
    url = "http://cr-tool.svc.res.ahl/api/live/query/incident/{}".format(query)
    cr_response = requests.get(url)
    if cr_response.status_code == 200:
        json_data = json.loads(cr_response.text)
        month_data.extend(json_data)
        
incidents = pd.DataFrame.from_records(month_data, columns=['number', 'opened_at', 'priority', 'impact', 'urgency', 'short_description', 'u_environment', 'close_code', 'close_notes'])
incidents['opened_at'] = pd.to_datetime(incidents['opened_at']).dt.date
# incidents['opened_at'] = incidents['opened_at'].dt.date
incidents['priority'] = incidents['priority'].map({3: 'P2', 4: 'P1'})
incidents['impact'] = incidents['impact'].map({1: 'Single User', 2: 'Multi Users'})    
incidents['urgency'] = incidents['urgency'].map({3: 'Urgent', 4: 'Critical'})
incidents
# -


