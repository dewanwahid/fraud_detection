# *************************************************
# Prioritized Fraud Detection Model
# *************************************************

# -----------------------------------
# Standard library imports
# -----------------------------------
import pandas as pd
# -----------------------------------------
# Internal library imports (util functions)
# -----------------------------------------

from src.deploy.fraud_risk_score import *

# -----------------------------------------
# GCP credentials
# -----------------------------------------
from google.oauth2 import service_account

from src.deploy.data_collection_from_gcp import *

credentials = service_account.Credentials.from_service_account_file(
    '//src/fb-data-science-dev-3dc8516990f6.json', )

project_id = "fb-data-science-dev"


# -----------------------------------------
# Prioritized Fraud Risk Accounts (FRA)
# -----------------------------------------

# Number of account that we want to report
n = 20

# Get the prioritized FRA
df_fra_top_N_for_support = prioritized_fra(credentials, project_id, n)

# -----------------------------------------
# Save the prioritized FRA list to local
# -----------------------------------------

# Path and file name for user accounts for need to be labeled by support team
path = "//data/"

# Output file name
file_name = "new_fra_tbl_"
today = str(date.today())
frc_tbl = path + file_name + today + ".csv"

# Save the user accounts for need to be labeled by support team
df_fra_top_N_for_support.to_csv(frc_tbl, sep=",", index=False)
