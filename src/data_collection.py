import pandas as pd
import numpy as np
import pandas_gbq

import os
from google.cloud.bigquery import client
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = " "
# Construct a BigQuery client object
client = bigquery.Client()

# Project and table id
table_id = "project and table id" 


