# -----------------------------------
# Standard library imports
# -----------------------------------
import pandas as pd
from datetime import date

# -----------------------------------------
# GCP credentials
# -----------------------------------------
import os
from google.cloud.bigquery import client
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = " "
# Construct a BigQuery client object
client = bigquery.Client()

# Project and table id
table_id = "project and table id"

# -----------------------------------------
# Labeled the prioritized FRA file path
# -----------------------------------------
pd.options.mode.chained_assignment = None  # default='warn'
# Path and file name for user accounts for need to be labeled by support team
path = "//data/"

# Output file name
file_name = "new_fra_tbl_"
today = str(date.today())
fra_path = path + file_name + today + ".csv"

# Read the labeled FRA as dataframe
df1 = pd.read_csv(fra_path, sep=",")  # read the labeled data
df2 = df1[df1.fraud_label != -1]  # get only labeled accounts

# Converting the date columns dtypes
df2['signup_date'] = pd.to_datetime(df2['signup_date'], format="%Y-%m-%d")
df2['effective_date'] = pd.to_datetime(df2['effective_date'], format="%Y-%m-%d")

# -----------------------------------------
# Define Schema
# -----------------------------------------
job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("invo_fea1", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("invo_fea2", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("date1", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("fraud_label", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("invo_fea3", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("support_note", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    # write_disposition="WRITE_TRUNCATE",
)

# -----------------------------------------
# Upload to BigQuery
# -----------------------------------------
job = client.load_table_from_dataframe(
    df2, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)


# -----------------------------------------
# Merge to local saved data
# -----------------------------------------
df_fra_labeled_local =\
    pd.read_csv('//data/fraud_non_fraud_labeled.csv', sep=",")

# Merging new labeled data with the old RL training data
df_fra_labeled_merged = df_fra_labeled_local.append(df2)
df_fra_labeled_merged = df_fra_labeled_merged.reset_index(drop=True)

# Save the merge list of fraud status labeled accounts labeled by support team
path_and_file_name2 = '//data/fraud_non_fraud_labeled.csv'
df_fra_labeled_merged.to_csv(path_and_file_name2, sep=",", index=False)
