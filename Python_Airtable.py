###
### Extract csv file data from airtabe into python
###
import pandas as pd
import json
import numpy as np
import string
import os
import string
import requests
import boto3
from io import StringIO


base_id = ""  # ENTER YOUR OWN BASE ID
table_name = ""  # ENTER TABLE NAME
url = "https://api.airtable.com/v0/" + base_id + "/" + table_name
api_key = ""  # ENTER API KEY
headers = {"Authorization": "Bearer " + api_key}
params = ()
cost_records = []
run = True
while run is True:
    response = requests.get(url, params=params, headers=headers)
    cost_response = response.json()
    cost_records += (cost_response['records'])
    if 'offset' in cost_response:
        run = True
        params = (('offset', cost_response['offset']),)
    else:
        run = False
cost_rows = []
for record in cost_records:
    cost_rows.append(record['fields'])
data=pd.DataFrame(cost_rows)
