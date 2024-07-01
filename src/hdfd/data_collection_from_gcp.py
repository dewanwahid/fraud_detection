# ---------------------------------------
# Standard library imports
# ---------------------------------------
from __builtin__ import list, set, sorted, range, len

import pandas as pd
import numpy as np
import pandas_gbq

# ---------------------------------------
# Internal library imports
# ---------------------------------------
from src.util_functions import *
from src.sql_query import *
from src.important_feature_list import *


# *************************************************************************
# Test data collection
# *************************************************************************


def test_data(credentials, project_id):
    # SQL BigQuery
    sql_test = sql_for_test_data()

    # Querying data set from GCP BQ
    df_test = pandas_gbq.read_gbq(sql_test, project_id=project_id, credentials=credentials)
    df_test.describe()

    print("test data")

    return df_test


# *************************************************************************
# Already labeled data collection
# *************************************************************************
def labeled_fraud_data(credentials, project_id):
    sql_fraud = sql_for_labeled_fraud_data()

    # Querying data set from GCP BQ
    df__labeled_fraud = pandas_gbq.read_gbq(sql_fraud, project_id=project_id, credentials=credentials)

    return df__labeled_fraud


# *************************************************************************
# Day 07: Data collection
# *************************************************************************
def day_7_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 7 days after signup_date
    sql_invoices_day_7 = sql_for_day_7_invoice_data()

    # Import as dataframe from redshift
    df_invoices_7days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_7, project_id=project_id,
                                                         credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_7days_all_accounts['avg_wc_description_day_7'] = df_invoices_7days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_7days_all_accounts['avg_wc_notes_day_7'] = df_invoices_7days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_7days_all_accounts['avg_wc_terms_day_7'] = df_invoices_7days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_7days_all_accounts['avg_wc_address_day_7'] = df_invoices_7days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_7days_all_accounts_fil = df_invoices_7days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_7',
         'avg_wc_notes_day_7', 'avg_wc_terms_day_7', 'avg_wc_address_day_7'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_7days_all_accounts_total = df_invoices_7days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_7days_all_accounts_final = df_word_count_7days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_7',
         'avg_wc_notes_day_7', 'avg_wc_terms_day_7', 'avg_wc_address_day_7'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_7 = sql_for_day_7_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_7, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_7 = sql_for_day_7_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_7, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 7
    df_events_all_accounts_day_7 = df_events_all_accounts[['id', 'event_count_day_7', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot the Day 7 Events (Each Unique Event Become a Column)

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_7 = \
        df_events_all_accounts_day_7.pivot_table(values='event_count_day_7', columns='evnt_nm', index='id',
                                                 aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_7.columns.name = None

    # Reset the index
    df_events_all_accounts_day_7 = df_events_all_accounts_day_7.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_7.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 7 period
    df_rs_events_day_7 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_7,
                                  on='id', how='left')

    # Merging average word count with 'df_rs_events_day_7'
    df_rs_events_avg_wc_day_7 = pd.merge(df_rs_events_day_7, df_word_count_7days_all_accounts_final, on='id',
                                         how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_7 = imp_features_day_7()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_7)):
        if imp_features_list_day_7[i] in df_rs_events_avg_wc_day_7.columns:
            continue

        else:
            # print("False: ", imp_features_list_day_7[i])
            df_rs_events_avg_wc_day_7[imp_features_list_day_7[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_7 = \
        df_rs_events_avg_wc_day_7[df_rs_events_avg_wc_day_7.columns.intersection(imp_features_list_day_7)]

    # Reindexing 
    df_imp_features_new_accounts_day_7 = df_imp_features_new_accounts_day_7.reindex(
        sorted(df_imp_features_new_accounts_day_7.columns), axis=1)

    # ----------------------------------------------------------------------
    # 7. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_7)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_7 = \
        df_imp_features_new_accounts_day_7[
            df_imp_features_new_accounts_day_7.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_7


# *************************************************************************
# Day 14: Data collection
# *************************************************************************

def day_14_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 14 days after signup_date
    sql_invoices_day_14 = sql_for_day_14_invoice_data()

    # Import as dataframe from redshift
    df_invoices_14days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_14, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_14days_all_accounts['avg_wc_description_day_14'] = df_invoices_14days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_14days_all_accounts['avg_wc_notes_day_14'] = df_invoices_14days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_14days_all_accounts['avg_wc_terms_day_14'] = df_invoices_14days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_14days_all_accounts['avg_wc_address_day_14'] = df_invoices_14days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_14days_all_accounts_fil = df_invoices_14days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_14',
         'avg_wc_notes_day_14', 'avg_wc_terms_day_14', 'avg_wc_address_day_14'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_14days_all_accounts_total = df_invoices_14days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_14days_all_accounts_final = df_word_count_14days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_14',
         'avg_wc_notes_day_14', 'avg_wc_terms_day_14', 'avg_wc_address_day_14'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_14 = sql_for_day_14_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_14, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_14 = sql_for_day_14_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_14, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 14
    df_events_all_accounts_day_14 = df_events_all_accounts[['id', 'event_count_day_14', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_14 = \
        df_events_all_accounts_day_14.pivot_table(values='event_count_day_14', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_14.columns.name = None

    # Reset the index
    df_events_all_accounts_day_14 = df_events_all_accounts_day_14.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_14.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 14 period
    df_rs_events_day_14 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_14,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_14'
    df_rs_events_avg_wc_day_14 = pd.merge(df_rs_events_day_14, df_word_count_14days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_14 = imp_features_day_14()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_14)):
        if imp_features_list_day_14[i] in df_rs_events_avg_wc_day_14.columns:
            continue

        else:
            # print("False: ", imp_features_list_day_14[i])
            df_rs_events_avg_wc_day_14[imp_features_list_day_14[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_14 = \
        df_rs_events_avg_wc_day_14[df_rs_events_avg_wc_day_14.columns.intersection(imp_features_list_day_14)]

    # Reindexing 
    df_imp_features_new_accounts_day_14 = df_imp_features_new_accounts_day_14.reindex(
        sorted(df_imp_features_new_accounts_day_14.columns), axis=1)

    # ----------------------------------------------------------------------
    # 14. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_14)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_14 = \
        df_imp_features_new_accounts_day_14[
            df_imp_features_new_accounts_day_14.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_14


# *************************************************************************    
# Day 21: Data collection
# *************************************************************************

def day_21_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 21 days after signup_date
    sql_invoices_day_21 = sql_for_day_21_invoice_data()

    # Import as dataframe from redshift
    df_invoices_21days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_21, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_21days_all_accounts['avg_wc_description_day_21'] = df_invoices_21days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_21days_all_accounts['avg_wc_notes_day_21'] = df_invoices_21days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_21days_all_accounts['avg_wc_terms_day_21'] = df_invoices_21days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_21days_all_accounts['avg_wc_address_day_21'] = df_invoices_21days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_21days_all_accounts_fil = df_invoices_21days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_21',
         'avg_wc_notes_day_21', 'avg_wc_terms_day_21', 'avg_wc_address_day_21'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_21days_all_accounts_total = df_invoices_21days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_21days_all_accounts_final = df_word_count_21days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_21',
         'avg_wc_notes_day_21', 'avg_wc_terms_day_21', 'avg_wc_address_day_21'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_21 = sql_for_day_21_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_21, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_21 = sql_for_day_21_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_21, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 21
    df_events_all_accounts_day_21 = df_events_all_accounts[['id', 'event_count_day_21', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_21 = \
        df_events_all_accounts_day_21.pivot_table(values='event_count_day_21', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_21.columns.name = None

    # Reset the index
    df_events_all_accounts_day_21 = df_events_all_accounts_day_21.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_21.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 21 period
    df_rs_events_day_21 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_21,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_21'
    df_rs_events_avg_wc_day_21 = pd.merge(df_rs_events_day_21, df_word_count_21days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_21 = imp_features_day_21()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_21)):
        if imp_features_list_day_21[i] in df_rs_events_avg_wc_day_21.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_21[i])
            df_rs_events_avg_wc_day_21[imp_features_list_day_21[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_21 = \
        df_rs_events_avg_wc_day_21[df_rs_events_avg_wc_day_21.columns.intersection(imp_features_list_day_21)]

    # Reindexing 
    df_imp_features_new_accounts_day_21 = df_imp_features_new_accounts_day_21.reindex(
        sorted(df_imp_features_new_accounts_day_21.columns), axis=1)

    # ----------------------------------------------------------------------
    # 21. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_21)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_21 = \
        df_imp_features_new_accounts_day_21[
            df_imp_features_new_accounts_day_21.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_21


# *************************************************************************
# Day 28: Data collection
# *************************************************************************

def day_28_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 28 days after signup_date
    sql_invoices_day_28 = sql_for_day_28_invoice_data()

    # Import as dataframe from redshift
    df_invoices_28days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_28, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_28days_all_accounts['avg_wc_description_day_28'] = df_invoices_28days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_28days_all_accounts['avg_wc_notes_day_28'] = df_invoices_28days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_28days_all_accounts['avg_wc_terms_day_28'] = df_invoices_28days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_28days_all_accounts['avg_wc_address_day_28'] = df_invoices_28days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_28days_all_accounts_fil = df_invoices_28days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_28',
         'avg_wc_notes_day_28', 'avg_wc_terms_day_28', 'avg_wc_address_day_28'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_28days_all_accounts_total = df_invoices_28days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_28days_all_accounts_final = df_word_count_28days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_28',
         'avg_wc_notes_day_28', 'avg_wc_terms_day_28', 'avg_wc_address_day_28'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_28 = sql_for_day_28_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_28, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_28 = sql_for_day_28_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_28, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 28
    df_events_all_accounts_day_28 = df_events_all_accounts[['id', 'event_count_day_28', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_28 = \
        df_events_all_accounts_day_28.pivot_table(values='event_count_day_28', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_28.columns.name = None

    # Reset the index
    df_events_all_accounts_day_28 = df_events_all_accounts_day_28.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_28.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 28 period
    df_rs_events_day_28 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_28,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_28'
    df_rs_events_avg_wc_day_28 = pd.merge(df_rs_events_day_28, df_word_count_28days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_28 = imp_features_day_28()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_28)):
        if imp_features_list_day_28[i] in df_rs_events_avg_wc_day_28.columns:
            continue

        else:
            # print("False: ", imp_features_list_day_28[i])
            df_rs_events_avg_wc_day_28[imp_features_list_day_28[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_28 = \
        df_rs_events_avg_wc_day_28[df_rs_events_avg_wc_day_28.columns.intersection(imp_features_list_day_28)]

    # Reindexing 
    df_imp_features_new_accounts_day_28 = df_imp_features_new_accounts_day_28.reindex(
        sorted(df_imp_features_new_accounts_day_28.columns), axis=1)

    # ----------------------------------------------------------------------
    # 28. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_28)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_28 = \
        df_imp_features_new_accounts_day_28[
            df_imp_features_new_accounts_day_28.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_28


# *************************************************************************
# Day 35: Data collection
# *************************************************************************

def day_35_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 35 days after signup_date
    sql_invoices_day_35 = sql_for_day_35_invoice_data()

    # Import as dataframe from redshift
    df_invoices_35days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_35, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_35days_all_accounts['avg_wc_description_day_35'] = df_invoices_35days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_35days_all_accounts['avg_wc_notes_day_35'] = df_invoices_35days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_35days_all_accounts['avg_wc_terms_day_35'] = df_invoices_35days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_35days_all_accounts['avg_wc_address_day_35'] = df_invoices_35days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_35days_all_accounts_fil = df_invoices_35days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_35',
         'avg_wc_notes_day_35', 'avg_wc_terms_day_35', 'avg_wc_address_day_35'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_35days_all_accounts_total = df_invoices_35days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_35days_all_accounts_final = df_word_count_35days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_35',
         'avg_wc_notes_day_35', 'avg_wc_terms_day_35', 'avg_wc_address_day_35'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_35 = sql_for_day_35_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_35, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_35 = sql_for_day_35_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_35, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 35
    df_events_all_accounts_day_35 = df_events_all_accounts[['id', 'event_count_day_35', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_35 = \
        df_events_all_accounts_day_35.pivot_table(values='event_count_day_35', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_35.columns.name = None

    # Reset the index
    df_events_all_accounts_day_35 = df_events_all_accounts_day_35.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_35.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 35 period
    df_rs_events_day_35 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_35,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_35'
    df_rs_events_avg_wc_day_35 = pd.merge(df_rs_events_day_35, df_word_count_35days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_35 = imp_features_day_35()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_35)):
        if imp_features_list_day_35[i] in df_rs_events_avg_wc_day_35.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_35[i])
            df_rs_events_avg_wc_day_35[imp_features_list_day_35[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_35 = \
        df_rs_events_avg_wc_day_35[df_rs_events_avg_wc_day_35.columns.intersection(imp_features_list_day_35)]

    # Reindexing 
    df_imp_features_new_accounts_day_35 = df_imp_features_new_accounts_day_35.reindex(
        sorted(df_imp_features_new_accounts_day_35.columns), axis=1)

    # ----------------------------------------------------------------------
    # 35. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_35)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_35 = \
        df_imp_features_new_accounts_day_35[
            df_imp_features_new_accounts_day_35.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_35


# *************************************************************************
# Day 42: Data collection
# *************************************************************************

def day_42_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 42 days after signup_date
    sql_invoices_day_42 = sql_for_day_42_invoice_data()

    # Import as dataframe from redshift
    df_invoices_42days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_42, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_42days_all_accounts['avg_wc_description_day_42'] = df_invoices_42days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_42days_all_accounts['avg_wc_notes_day_42'] = df_invoices_42days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_42days_all_accounts['avg_wc_terms_day_42'] = df_invoices_42days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_42days_all_accounts['avg_wc_address_day_42'] = df_invoices_42days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_42days_all_accounts_fil = df_invoices_42days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_42',
         'avg_wc_notes_day_42', 'avg_wc_terms_day_42', 'avg_wc_address_day_42'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_42days_all_accounts_total = df_invoices_42days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_42days_all_accounts_final = df_word_count_42days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_42',
         'avg_wc_notes_day_42', 'avg_wc_terms_day_42', 'avg_wc_address_day_42'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_42 = sql_for_day_42_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_42, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_42 = sql_for_day_42_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_42, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 42
    df_events_all_accounts_day_42 = df_events_all_accounts[['id', 'event_count_day_42', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_42 = \
        df_events_all_accounts_day_42.pivot_table(values='event_count_day_42', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_42.columns.name = None

    # Reset the index
    df_events_all_accounts_day_42 = df_events_all_accounts_day_42.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_42.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 42 period
    df_rs_events_day_42 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_42,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_42'
    df_rs_events_avg_wc_day_42 = pd.merge(df_rs_events_day_42, df_word_count_42days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_42 = imp_features_day_42()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_42)):
        if imp_features_list_day_42[i] in df_rs_events_avg_wc_day_42.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_42[i])
            df_rs_events_avg_wc_day_42[imp_features_list_day_42[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_42 = \
        df_rs_events_avg_wc_day_42[df_rs_events_avg_wc_day_42.columns.intersection(imp_features_list_day_42)]

    # Reindexing 
    df_imp_features_new_accounts_day_42 = df_imp_features_new_accounts_day_42.reindex(
        sorted(df_imp_features_new_accounts_day_42.columns), axis=1)

    # ----------------------------------------------------------------------
    # 42. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_42)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_42 = \
        df_imp_features_new_accounts_day_42[
            df_imp_features_new_accounts_day_42.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_42


# *************************************************************************
# Day 49: Data collection
# *************************************************************************

def day_49_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 49 days after signup_date
    sql_invoices_day_49 = sql_for_day_49_invoice_data()

    # Import as dataframe from redshift
    df_invoices_49days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_49, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_49days_all_accounts['avg_wc_description_day_49'] = df_invoices_49days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_49days_all_accounts['avg_wc_notes_day_49'] = df_invoices_49days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_49days_all_accounts['avg_wc_terms_day_49'] = df_invoices_49days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_49days_all_accounts['avg_wc_address_day_49'] = df_invoices_49days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_49days_all_accounts_fil = df_invoices_49days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_49',
         'avg_wc_notes_day_49', 'avg_wc_terms_day_49', 'avg_wc_address_day_49'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_49days_all_accounts_total = df_invoices_49days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_49days_all_accounts_final = df_word_count_49days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_49',
         'avg_wc_notes_day_49', 'avg_wc_terms_day_49', 'avg_wc_address_day_49'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_49 = sql_for_day_49_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_49, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_49 = sql_for_day_49_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_49, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 49
    df_events_all_accounts_day_49 = df_events_all_accounts[['id', 'event_count_day_49', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_49 = \
        df_events_all_accounts_day_49.pivot_table(values='event_count_day_49', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_49.columns.name = None

    # Reset the index
    df_events_all_accounts_day_49 = df_events_all_accounts_day_49.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_49.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 49 period
    df_rs_events_day_49 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_49,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_49'
    df_rs_events_avg_wc_day_49 = pd.merge(df_rs_events_day_49, df_word_count_49days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_49 = imp_features_day_49()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_49)):
        if imp_features_list_day_49[i] in df_rs_events_avg_wc_day_49.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_49[i])
            df_rs_events_avg_wc_day_49[imp_features_list_day_49[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_49 = \
        df_rs_events_avg_wc_day_49[df_rs_events_avg_wc_day_49.columns.intersection(imp_features_list_day_49)]

    # Reindexing 
    df_imp_features_new_accounts_day_49 = df_imp_features_new_accounts_day_49.reindex(
        sorted(df_imp_features_new_accounts_day_49.columns), axis=1)

    # ----------------------------------------------------------------------
    # 49. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_49)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_49 = \
        df_imp_features_new_accounts_day_49[
            df_imp_features_new_accounts_day_49.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_49


# *************************************************************************
# Day 56: Data collection
# *************************************************************************

def day_56_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 56 days after signup_date
    sql_invoices_day_56 = sql_for_day_56_invoice_data()

    # Import as dataframe from redshift
    df_invoices_56days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_56, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_56days_all_accounts['avg_wc_description_day_56'] = df_invoices_56days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_56days_all_accounts['avg_wc_notes_day_56'] = df_invoices_56days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_56days_all_accounts['avg_wc_terms_day_56'] = df_invoices_56days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_56days_all_accounts['avg_wc_address_day_56'] = df_invoices_56days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_56days_all_accounts_fil = df_invoices_56days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_56',
         'avg_wc_notes_day_56', 'avg_wc_terms_day_56', 'avg_wc_address_day_56'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_56days_all_accounts_total = df_invoices_56days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_56days_all_accounts_final = df_word_count_56days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_56',
         'avg_wc_notes_day_56', 'avg_wc_terms_day_56', 'avg_wc_address_day_56'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_56 = sql_for_day_56_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_56, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_56 = sql_for_day_56_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_56, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 56
    df_events_all_accounts_day_56 = df_events_all_accounts[['id', 'event_count_day_56', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_56 = \
        df_events_all_accounts_day_56.pivot_table(values='event_count_day_56', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_56.columns.name = None

    # Reset the index
    df_events_all_accounts_day_56 = df_events_all_accounts_day_56.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_56.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 56 period
    df_rs_events_day_56 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_56,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_56'
    df_rs_events_avg_wc_day_56 = pd.merge(df_rs_events_day_56, df_word_count_56days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_56 = imp_features_day_56()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_56)):
        if imp_features_list_day_56[i] in df_rs_events_avg_wc_day_56.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_56[i])
            df_rs_events_avg_wc_day_56[imp_features_list_day_56[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_56 = \
        df_rs_events_avg_wc_day_56[df_rs_events_avg_wc_day_56.columns.intersection(imp_features_list_day_56)]

    # Reindexing 
    df_imp_features_new_accounts_day_56 = df_imp_features_new_accounts_day_56.reindex(
        sorted(df_imp_features_new_accounts_day_56.columns), axis=1)

    # ----------------------------------------------------------------------
    # 56. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_56)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_56 = \
        df_imp_features_new_accounts_day_56[
            df_imp_features_new_accounts_day_56.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_56


# *************************************************************************
# Day 63: Data collection
# *************************************************************************

def day_63_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 63 days after signup_date
    sql_invoices_day_63 = sql_for_day_63_invoice_data()

    # Import as dataframe from redshift
    df_invoices_63days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_63, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_63days_all_accounts['avg_wc_description_day_63'] = df_invoices_63days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_63days_all_accounts['avg_wc_notes_day_63'] = df_invoices_63days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_63days_all_accounts['avg_wc_terms_day_63'] = df_invoices_63days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_63days_all_accounts['avg_wc_address_day_63'] = df_invoices_63days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_63days_all_accounts_fil = df_invoices_63days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_63',
         'avg_wc_notes_day_63', 'avg_wc_terms_day_63', 'avg_wc_address_day_63'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_63days_all_accounts_total = df_invoices_63days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_63days_all_accounts_final = df_word_count_63days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_63',
         'avg_wc_notes_day_63', 'avg_wc_terms_day_63', 'avg_wc_address_day_63'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_63 = sql_for_day_63_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_63, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_63 = sql_for_day_63_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_63, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 63
    df_events_all_accounts_day_63 = df_events_all_accounts[['id', 'event_count_day_63', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_63 = \
        df_events_all_accounts_day_63.pivot_table(values='event_count_day_63', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_63.columns.name = None

    # Reset the index
    df_events_all_accounts_day_63 = df_events_all_accounts_day_63.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_63.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 63 period
    df_rs_events_day_63 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_63,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_63'
    df_rs_events_avg_wc_day_63 = pd.merge(df_rs_events_day_63, df_word_count_63days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_63 = imp_features_day_63()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_63)):
        if imp_features_list_day_63[i] in df_rs_events_avg_wc_day_63.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_63[i])
            df_rs_events_avg_wc_day_63[imp_features_list_day_63[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_63 = \
        df_rs_events_avg_wc_day_63[df_rs_events_avg_wc_day_63.columns.intersection(imp_features_list_day_63)]

    # Reindexing 
    df_imp_features_new_accounts_day_63 = df_imp_features_new_accounts_day_63.reindex(
        sorted(df_imp_features_new_accounts_day_63.columns), axis=1)

    # ----------------------------------------------------------------------
    # 63. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_63)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_63 = \
        df_imp_features_new_accounts_day_63[
            df_imp_features_new_accounts_day_63.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_63


# *************************************************************************
# Day 70: Data collection
# *************************************************************************

def day_70_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 70 days after signup_date
    sql_invoices_day_70 = sql_for_day_70_invoice_data()

    # Import as dataframe from redshift
    df_invoices_70days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_70, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_70days_all_accounts['avg_wc_description_day_70'] = df_invoices_70days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_70days_all_accounts['avg_wc_notes_day_70'] = df_invoices_70days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_70days_all_accounts['avg_wc_terms_day_70'] = df_invoices_70days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_70days_all_accounts['avg_wc_address_day_70'] = df_invoices_70days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_70days_all_accounts_fil = df_invoices_70days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_70',
         'avg_wc_notes_day_70', 'avg_wc_terms_day_70', 'avg_wc_address_day_70'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_70days_all_accounts_total = df_invoices_70days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_70days_all_accounts_final = df_word_count_70days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_70',
         'avg_wc_notes_day_70', 'avg_wc_terms_day_70', 'avg_wc_address_day_70'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_70 = sql_for_day_70_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_70, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_70 = sql_for_day_70_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_70, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 70
    df_events_all_accounts_day_70 = df_events_all_accounts[['id', 'event_count_day_70', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_70 = \
        df_events_all_accounts_day_70.pivot_table(values='event_count_day_70', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_70.columns.name = None

    # Reset the index
    df_events_all_accounts_day_70 = df_events_all_accounts_day_70.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_70.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 70 period
    df_rs_events_day_70 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_70,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_70'
    df_rs_events_avg_wc_day_70 = pd.merge(df_rs_events_day_70, df_word_count_70days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_70 = imp_features_day_70()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_70)):
        if imp_features_list_day_70[i] in df_rs_events_avg_wc_day_70.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_70[i])
            df_rs_events_avg_wc_day_70[imp_features_list_day_70[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_70 = \
        df_rs_events_avg_wc_day_70[df_rs_events_avg_wc_day_70.columns.intersection(imp_features_list_day_70)]

    # Reindexing 
    df_imp_features_new_accounts_day_70 = df_imp_features_new_accounts_day_70.reindex(
        sorted(df_imp_features_new_accounts_day_70.columns), axis=1)

    # ----------------------------------------------------------------------
    # 70. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_70)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_70 = \
        df_imp_features_new_accounts_day_70[
            df_imp_features_new_accounts_day_70.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_70


# *************************************************************************
# Day 77: Data collection
# *************************************************************************

def day_77_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 77 days after signup_date
    sql_invoices_day_77 = sql_for_day_77_invoice_data()

    # Import as dataframe from redshift
    df_invoices_77days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_77, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_77days_all_accounts['avg_wc_description_day_77'] = df_invoices_77days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_77days_all_accounts['avg_wc_notes_day_77'] = df_invoices_77days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_77days_all_accounts['avg_wc_terms_day_77'] = df_invoices_77days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_77days_all_accounts['avg_wc_address_day_77'] = df_invoices_77days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_77days_all_accounts_fil = df_invoices_77days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_77',
         'avg_wc_notes_day_77', 'avg_wc_terms_day_77', 'avg_wc_address_day_77'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_77days_all_accounts_total = df_invoices_77days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_77days_all_accounts_final = df_word_count_77days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_77',
         'avg_wc_notes_day_77', 'avg_wc_terms_day_77', 'avg_wc_address_day_77'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_77 = sql_for_day_77_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_77, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_77 = sql_for_day_77_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_77, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 77
    df_events_all_accounts_day_77 = df_events_all_accounts[['id', 'event_count_day_77', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_77 = \
        df_events_all_accounts_day_77.pivot_table(values='event_count_day_77', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_77.columns.name = None

    # Reset the index
    df_events_all_accounts_day_77 = df_events_all_accounts_day_77.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_77.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 77 period
    df_rs_events_day_77 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_77,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_77'
    df_rs_events_avg_wc_day_77 = pd.merge(df_rs_events_day_77, df_word_count_77days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_77 = imp_features_day_77()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_77)):
        if imp_features_list_day_77[i] in df_rs_events_avg_wc_day_77.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_77[i])
            df_rs_events_avg_wc_day_77[imp_features_list_day_77[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_77 = \
        df_rs_events_avg_wc_day_77[df_rs_events_avg_wc_day_77.columns.intersection(imp_features_list_day_77)]

    # Reindexing 
    df_imp_features_new_accounts_day_77 = df_imp_features_new_accounts_day_77.reindex(
        sorted(df_imp_features_new_accounts_day_77.columns), axis=1)

    # ----------------------------------------------------------------------
    # 77. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_77)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_77 = \
        df_imp_features_new_accounts_day_77[
            df_imp_features_new_accounts_day_77.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_77


# *************************************************************************
# Day 84: Data collection
# *************************************************************************

def day_84_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 84 days after signup_date
    sql_invoices_day_84 = sql_for_day_84_invoice_data()

    # Import as dataframe from redshift
    df_invoices_84days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_84, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_84days_all_accounts['avg_wc_description_day_84'] = df_invoices_84days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_84days_all_accounts['avg_wc_notes_day_84'] = df_invoices_84days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_84days_all_accounts['avg_wc_terms_day_84'] = df_invoices_84days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_84days_all_accounts['avg_wc_address_day_84'] = df_invoices_84days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_84days_all_accounts_fil = df_invoices_84days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_84',
         'avg_wc_notes_day_84', 'avg_wc_terms_day_84', 'avg_wc_address_day_84'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_84days_all_accounts_total = df_invoices_84days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_84days_all_accounts_final = df_word_count_84days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_84',
         'avg_wc_notes_day_84', 'avg_wc_terms_day_84', 'avg_wc_address_day_84'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_84 = sql_for_day_84_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_84, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_84 = sql_for_day_84_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_84, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 84
    df_events_all_accounts_day_84 = df_events_all_accounts[['id', 'event_count_day_84', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot the Day 84 Events (Each Unique Event Become a Column)

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_84 = \
        df_events_all_accounts_day_84.pivot_table(values='event_count_day_84', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_84.columns.name = None

    # Reset the index
    df_events_all_accounts_day_84 = df_events_all_accounts_day_84.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_84.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 84 period
    df_rs_events_day_84 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_84,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_84'
    df_rs_events_avg_wc_day_84 = pd.merge(df_rs_events_day_84, df_word_count_84days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_84 = imp_features_day_84()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_84)):
        if imp_features_list_day_84[i] in df_rs_events_avg_wc_day_84.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_84[i])
            df_rs_events_avg_wc_day_84[imp_features_list_day_84[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_84 = \
        df_rs_events_avg_wc_day_84[df_rs_events_avg_wc_day_84.columns.intersection(imp_features_list_day_84)]

    # Reindexing 
    df_imp_features_new_accounts_day_84 = df_imp_features_new_accounts_day_84.reindex(
        sorted(df_imp_features_new_accounts_day_84.columns), axis=1)

    # ----------------------------------------------------------------------
    # 84. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_84)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_84 = \
        df_imp_features_new_accounts_day_84[
            df_imp_features_new_accounts_day_84.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_84


# *************************************************************************
# Day 91: Data collection
# *************************************************************************

def day_91_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 91 days after signup_date
    sql_invoices_day_91 = sql_for_day_91_invoice_data()

    # Import as dataframe from redshift
    df_invoices_91days_all_accounts = pandas_gbq.read_gbq(sql_invoices_day_91, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's description, notes, terms, address
    df_invoices_91days_all_accounts['avg_wc_description_day_91'] = df_invoices_91days_all_accounts.apply(
        lambda x: words_count(x['description']), axis=1)
    df_invoices_91days_all_accounts['avg_wc_notes_day_91'] = df_invoices_91days_all_accounts.apply(
        lambda x: words_count(x['notes']), axis=1)
    df_invoices_91days_all_accounts['avg_wc_terms_day_91'] = df_invoices_91days_all_accounts.apply(
        lambda x: words_count(x['terms']), axis=1)
    df_invoices_91days_all_accounts['avg_wc_address_day_91'] = df_invoices_91days_all_accounts.apply(
        lambda x: words_count(x['address']), axis=1)

    # Filters the text columns from the dataframe
    df_invoices_91days_all_accounts_fil = df_invoices_91days_all_accounts.filter(
        ['id', 'invoiceid', 'signup_date', 'create_date',
         'created_at', 'days_to_invoice_creation', 'avg_wc_description_day_91',
         'avg_wc_notes_day_91', 'avg_wc_terms_day_91', 'avg_wc_address_day_91'])

    # Summing (grouping) all invoices for a 'id'
    df_word_count_91days_all_accounts_total = df_invoices_91days_all_accounts_fil.groupby('id').mean()

    # Final word count table
    df_word_count_91days_all_accounts_final = df_word_count_91days_all_accounts_total.filter(
        ['id', 'signup_date', 'avg_wc_description_day_91',
         'avg_wc_notes_day_91', 'avg_wc_terms_day_91', 'avg_wc_address_day_91'])

    # ----------------------------------------------------------------------
    # 2. Report Systems Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_day_91 = sql_for_day_91_rs_data()

    # Import as dataframe from GCP
    df_rs_invoices_clients_activities_all_accounts = \
        pandas_gbq.read_gbq(sql_rs_day_91, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from Events Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. Event data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for events 
    sql_events_day_91 = sql_for_day_91_event()

    # Import as dataframe from bq
    df_events_all_accounts = pandas_gbq.read_gbq(sql_events_day_91, project_id=project_id, credentials=credentials)

    # Removing whitespace from the event strings
    # Removing row if there is 'None' the event cell
    df_events_all_accounts = df_events_all_accounts[~df_events_all_accounts.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df_events_all_accounts.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the event string name
    df_events_all_accounts['evnt_nm'] = \
        df_events_all_accounts.apply(lambda x: x['event'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the events columns for day 91
    df_events_all_accounts_day_91 = df_events_all_accounts[['id', 'evnt_ct', 'evnt_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the Events (each unique event become a column)
    # ----------------------------------------------------------------------

    # Pivot the Day 91 Events (Each Unique Event Become a Column)

    # Pivot table based on the unique column value in 'evnt_nm'
    df_events_all_accounts_day_91 = \
        df_events_all_accounts_day_91.pivot_table(values='evnt_ct', columns='evnt_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df_events_all_accounts_day_91.columns.name = None

    # Reset the index
    df_events_all_accounts_day_91 = df_events_all_accounts_day_91.reset_index()

    # Replace 'NaN' with zero
    df_events_all_accounts_day_91.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: Report system, average word count and event data
    # ----------------------------------------------------------------------
    # Merging report system and events data for day 91 period
    df_rs_events_day_91 = pd.merge(df_rs_invoices_clients_activities_all_accounts, df_events_all_accounts_day_91,
                                   on='id', how='left')

    # Merging average word count with 'df_rs_events_day_91'
    df_rs_events_avg_wc_day_91 = pd.merge(df_rs_events_day_91, df_word_count_91days_all_accounts_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_day_91 = imp_features_day_91()

    # Adding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_day_91)):
        if imp_features_list_day_91[i] in df_rs_events_avg_wc_day_91.columns:
            continue
        else:
            # print("False: ", imp_features_list_day_91[i])
            df_rs_events_avg_wc_day_91[imp_features_list_day_91[i]] = 0

    # Filtering only important features 
    df_imp_features_new_accounts_day_91 = \
        df_rs_events_avg_wc_day_91[df_rs_events_avg_wc_day_91.columns.intersection(imp_features_list_day_91)]

    # Reindexing 
    df_imp_features_new_accounts_day_91 = df_imp_features_new_accounts_day_91.reindex(
        sorted(df_imp_features_new_accounts_day_91.columns), axis=1)

    # ----------------------------------------------------------------------
    # 91. Filtering inactive users' accounts
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['email', 'days_on_platform', 'effective_date', 'signup_date', 'id']
    cols_list = list(df_imp_features_new_accounts_day_91)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users accounts
    df_final_features_new_accounts_day_91 = \
        df_imp_features_new_accounts_day_91[
            df_imp_features_new_accounts_day_91.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new accounts
    # ----------------------------------------------------------------------

    return df_final_features_new_accounts_day_91
