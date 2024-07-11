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
# Alreinvo_str4_y labeled data collection
# *************************************************************************
def labeled_fraud_data(credentials, project_id):
    sql_fraud = sql_for_labeled_fraud_data()

    # Querying data set from GCP BQ
    df1 = pandas_gbq.invo_gbq(sql_fraud, project_id=project_id, credentials=credentials)

    return df1


# *************************************************************************
# dct 07: Data collection
# *************************************************************************
def dct_7_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 7 dcts after date1
    sql = sql_d7_invoice_data()

    # Import as dataframe from redshift
    df2 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                         credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df2['invo_str1_d7'] = df2.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df2['invo_str2_d7'] = df2.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df2['invo_str3_d7'] = df2.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df2['invo_str4_d7'] = df2.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df2_fil = df2.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d7',
         'invo_str2_d7', 'invo_str3_d7', 'invo_str4_d7'])

    # Summing (grouping) all invoices for a 'id'
    df3 = df2_fil.groupby('id').mean()

    # Final word count table
    df4 = df3.filter(
        ['id', 'date1', 'invo_str1_d7',
         'invo_str2_d7', 'invo_str3_d7', 'invo_str4_d7'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d7_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d7_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 7
    df5_dct_7 = df5[['id', 'invo_str3_ct_d7', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------
    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_7 =  df5_dct_7.pivot_table(values='invo_str3_ct_d7', columns='invo_str3_nm', index='id',
                                                 aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_7.columns.name = None

    # Reset the index
    df5_dct_7 = df5_dct_7.reset_index()

    # Replace 'NaN' with zero
    df5_dct_7.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 7 period
    df6 = pd.merge(df5, df5_dct_7,
                                  on='id', how='left')

    # Merging average word count with 'df6'
    df7 = pd.merge(df6, df4, on='id',
                                         how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_7 = imp_features_dct_7()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_7)):
        if imp_features_list_dct_7[i] in df7.columns:
            continue

        else:
            # print("False: ", imp_features_list_dct_7[i])
            df7[imp_features_list_dct_7[i]] = 0

    # Filtering only important features 
    df8 = df7[df7.columns.intersection(imp_features_list_dct_7)]

    # Reindexing 
    df8 = df8.reindex(sorted(df8.columns), axis=1)

    # ----------------------------------------------------------------------
    # 7. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df8)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df9 = df8[df8.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df9


# *************************************************************************
# dct 14: Data collection
# *************************************************************************

def dct_14_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 14 dcts after date1
    sql = sql_d14_invoice_data()

    # Import as dataframe from redshift
    df10 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df10['invo_str1_d14'] = df10.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df10['invo_str2_d14'] = df10.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df10['invo_str3_d14'] = df10.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df10['invo_str4_d14'] = df10.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df10_fil = df10.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d14',
         'invo_str2_d14', 'invo_str3_d14', 'invo_str4_d14'])

    # Summing (grouping) all invoices for a 'id'
    df11 = df10_fil.groupby('id').mean()

    # Final word count table
    df12 = df11.filter(['id', 'date1', 'invo_str1_d14','invo_str2_d14', 'invo_str3_d14', 'invo_str4_d14'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d14_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d14_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = \
        df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 14
    df5_dct_14 = df5[['id', 'invo_str3_ct_d14', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_14 =  df5_dct_14.pivot_table(values='invo_str3_ct_d14', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_14.columns.name = None

    # Reset the index
    df5_dct_14 = df5_dct_14.reset_index()

    # Replace 'NaN' with zero
    df5_dct_14.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 14 period
    df13 = pd.merge(df5, df5_dct_14,
                                   on='id', how='left')

    # Merging average word count with 'df13'
    df14 = pd.merge(df13, df12, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_14 = imp_features_dct_14()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_14)):
        if imp_features_list_dct_14[i] in df14.columns:
            continue

        else:
            # print("False: ", imp_features_list_dct_14[i])
            df14[imp_features_list_dct_14[i]] = 0

    # Filtering only important features 
    df15 = df14[df14.columns.intersection(imp_features_list_dct_14)]

    # Reindexing 
    df15 = df15.reindex(
        sorted(df15.columns), axis=1)

    # ----------------------------------------------------------------------
    # 14. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df15)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df16 = df15[df15.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------
    return df16


# *************************************************************************    
# dct 21: Data collection
# *************************************************************************

def dct_21_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 21 dcts after date1
    sql = sql_d21_invoice_data()

    # Import as dataframe from redshift
    df17 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df17['invo_str1_d21'] = df17.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df17['invo_str2_d21'] = df17.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df17['invo_str3_d21'] = df17.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df17['invo_str4_d21'] = df17.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df17_fil = df17.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d21',
         'invo_str2_d21', 'invo_str3_d21', 'invo_str4_d21'])

    # Summing (grouping) all invoices for a 'id'
    df18 = df17_fil.groupby('id').mean()

    # Final word count table
    df19 = df18.filter(['id', 'date1', 'invo_str1_d21', 'invo_str2_d21', 'invo_str3_d21', 'invo_str4_d21'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d21_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d21_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] =  df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 21
    df5_dct_21 = df5[['id', 'invo_str3_ct_d21', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_21 = df5_dct_21.pivot_table(values='invo_str3_ct_d21', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_21.columns.name = None

    # Reset the index
    df5_dct_21 = df5_dct_21.reset_index()

    # Replace 'NaN' with zero
    df5_dct_21.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 21 period
    df20 = pd.merge(df5, df5_dct_21,
                                   on='id', how='left')

    # Merging average word count with 'df20'
    df21 = pd.merge(df20, df19, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_21 = imp_features_dct_21()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_21)):
        if imp_features_list_dct_21[i] in df21.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_21[i])
            df21[imp_features_list_dct_21[i]] = 0

    # Filtering only important features 
    df22 = df21[df21.columns.intersection(imp_features_list_dct_21)]

    # Reindexing 
    df22 = df22.reindex(
        sorted(df22.columns), axis=1)

    # ----------------------------------------------------------------------
    # 21. Filtering inactive users' acc
    # ----------------------------------------------------------------------
    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df22)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df23 = df22[df22.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df23


# *************************************************************************
# dct 28: Data collection
# *************************************************************************

def dct_28_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 28 dcts after date1
    sql = sql_d28_invoice_data()

    # Import as dataframe from redshift
    df24 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df24['invo_str1_d28'] = df24.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df24['invo_str2_d28'] = df24.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df24['invo_str3_d28'] = df24.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df24['invo_str4_d28'] = df24.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df24_fil = df24.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d28',
         'invo_str2_d28', 'invo_str3_d28', 'invo_str4_d28'])

    # Summing (grouping) all invoices for a 'id'
    df25 = df24_fil.groupby('id').mean()

    # Final word count table
    df_word_count_28dcts_all_acc_final = df25.filter(
        ['id', 'date1', 'invo_str1_d28',
         'invo_str2_d28', 'invo_str3_d28', 'invo_str4_d28'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d28_rs_data()

    # Import as dataframe from GCP
    df5 =  pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d28_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 28
    df5_dct_28 = df5[['id', 'invo_str3_ct_d28', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_28 = df5_dct_28.pivot_table(values='invo_str3_ct_d28', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_28.columns.name = None

    # Reset the index
    df5_dct_28 = df5_dct_28.reset_index()

    # Replace 'NaN' with zero
    df5_dct_28.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 28 period
    df26 = pd.merge(df5, df5_dct_28,
                                   on='id', how='left')

    # Merging average word count with 'df26'
    df27 = pd.merge(df26, df_word_count_28dcts_all_acc_final, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_28 = imp_features_dct_28()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_28)):
        if imp_features_list_dct_28[i] in df27.columns:
            continue

        else:
            # print("False: ", imp_features_list_dct_28[i])
            df27[imp_features_list_dct_28[i]] = 0

    # Filtering only important features 
    df28 = df27[df27.columns.intersection(imp_features_list_dct_28)]

    # Reindexing 
    df28 = df28.reindex(
        sorted(df28.columns), axis=1)

    # ----------------------------------------------------------------------
    # 28. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df28)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df29 = df28[
            df28.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df29


# *************************************************************************
# dct 35: Data collection
# *************************************************************************

def dct_35_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 35 dcts after date1
    sql = sql_d35_invoice_data()

    # Import as dataframe from redshift
    df30 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df30['invo_str1_d35'] = df30.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df30['invo_str2_d35'] = df30.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df30['invo_str3_d35'] = df30.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df30['invo_str4_d35'] = df30.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df30_fil = df30.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d35',
         'invo_str2_d35', 'invo_str3_d35', 'invo_str4_d35'])

    # Summing (grouping) all invoices for a 'id'
    df31 = df30_fil.groupby('id').mean()

    # Final word count table
    df32 = df31.filter(
        ['id', 'date1', 'invo_str1_d35',
         'invo_str2_d35', 'invo_str3_d35', 'invo_str4_d35'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_dct_35 = sql_d35_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql_rs_dct_35, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d35_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = \
        df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 35
    df5_dct_35 = df5[['id', 'invo_str3_ct_d35', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_35 = df5_dct_35.pivot_table(values='invo_str3_ct_d35', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_35.columns.name = None

    # Reset the index
    df5_dct_35 = df5_dct_35.reset_index()

    # Replace 'NaN' with zero
    df5_dct_35.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 35 period
    df33 = pd.merge(df5, df5_dct_35,
                                   on='id', how='left')

    # Merging average word count with 'df33'
    df34 = pd.merge(df33, df32, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_35 = imp_features_dct_35()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_35)):
        if imp_features_list_dct_35[i] in df34.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_35[i])
            df34[imp_features_list_dct_35[i]] = 0

    # Filtering only important features 
    df35 = df34[df34.columns.intersection(imp_features_list_dct_35)]

    # Reindexing 
    df35 = df35.reindex(
        sorted(df35.columns), axis=1)

    # ----------------------------------------------------------------------
    # 35. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df35)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df36 = df35[df35.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df36


# *************************************************************************
# dct 42: Data collection
# *************************************************************************

def dct_42_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 42 dcts after date1
    sql = sql_d42_invoice_data()

    # Import as dataframe from redshift
    df37 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df37['invo_str1_d42'] = df37.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df37['invo_str2_d42'] = df37.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df37['invo_str3_d42'] = df37.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df37['invo_str4_d42'] = df37.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df37_fil = df37.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d42',
         'invo_str2_d42', 'invo_str3_d42', 'invo_str4_d42'])

    # Summing (grouping) all invoices for a 'id'
    df38 = df37_fil.groupby('id').mean()

    # Final word count table
    df39 = df38.filter(
        ['id', 'date1', 'invo_str1_d42',
         'invo_str2_d42', 'invo_str3_d42', 'invo_str4_d42'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d42_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d42_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 42
    df5_dct_42 = df5[['id', 'invo_str3_ct_d42', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_42 = df5_dct_42.pivot_table(values='invo_str3_ct_d42', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_42.columns.name = None

    # Reset the index
    df5_dct_42 = df5_dct_42.reset_index()

    # Replace 'NaN' with zero
    df5_dct_42.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 42 period
    df40 = pd.merge(df5, df5_dct_42,
                                   on='id', how='left')

    # Merging average word count with 'df40'
    df41 = pd.merge(df40, df39, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_42 = imp_features_dct_42()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_42)):
        if imp_features_list_dct_42[i] in df41.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_42[i])
            df41[imp_features_list_dct_42[i]] = 0

    # Filtering only important features 
    df42 = df41[df41.columns.intersection(imp_features_list_dct_42)]

    # Reindexing 
    df42 = df42.reindex(
        sorted(df42.columns), axis=1)

    # ----------------------------------------------------------------------
    # 42. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df42)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df43 = df42[df42.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df43


# *************************************************************************
# dct 49: Data collection
# *************************************************************************

def dct_49_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 49 dcts after date1
    sql = sql_d49_invoice_data()

    # Import as dataframe from redshift
    df44 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df44['invo_str1_d49'] = df44.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df44['invo_str2_d49'] = df44.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df44['invo_str3_d49'] = df44.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df44['invo_str4_d49'] = df44.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df44_fil = df44.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d49',
         'invo_str2_d49', 'invo_str3_d49', 'invo_str4_d49'])

    # Summing (grouping) all invoices for a 'id'
    df45 = df44_fil.groupby('id').mean()

    # Final word count table
    df46 = df45.filter(
        ['id', 'date1', 'invo_str1_d49',
         'invo_str2_d49', 'invo_str3_d49', 'invo_str4_d49'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d49_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d49_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 49
    df5_dct_49 = df5[['id', 'invo_str3_ct_d49', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_49 = df5_dct_49.pivot_table(values='invo_str3_ct_d49', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_49.columns.name = None

    # Reset the index
    df5_dct_49 = df5_dct_49.reset_index()

    # Replace 'NaN' with zero
    df5_dct_49.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 49 period
    df47 = pd.merge(df5, df5_dct_49,
                                   on='id', how='left')

    # Merging average word count with 'df47'
    df48 = pd.merge(df47, df46, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_49 = imp_features_dct_49()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_49)):
        if imp_features_list_dct_49[i] in df48.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_49[i])
            df48[imp_features_list_dct_49[i]] = 0

    # Filtering only important features 
    df49 = df48[df48.columns.intersection(imp_features_list_dct_49)]

    # Reindexing 
    df49 = df49.reindex(
        sorted(df49.columns), axis=1)

    # ----------------------------------------------------------------------
    # 49. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df49)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df50 = df49[df49.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df50


# *************************************************************************
# dct 56: Data collection
# *************************************************************************

def dct_56_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 56 dcts after date1
    sql_invoices_dct_56 = sql_d56_invoice_data()

    # Import as dataframe from redshift
    df51 = pandas_gbq.invo_gbq(sql_invoices_dct_56, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df51['invo_str1_d56'] = df51.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df51['invo_str2_d56'] = df51.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df51['invo_str3_d56'] = df51.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df51['invo_str4_d56'] = df51.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df51_fil = df51.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d56',
         'invo_str2_d56', 'invo_str3_d56', 'invo_str4_d56'])

    # Summing (grouping) all invoices for a 'id'
    df52 = df51_fil.groupby('id').mean()

    # Final word count table
    df53 = df52.filter(
        ['id', 'date1', 'invo_str1_d56',
         'invo_str2_d56', 'invo_str3_d56', 'invo_str4_d56'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d56_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d56_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = \
        df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 56
    df5_dct_56 = df5[['id', 'invo_str3_ct_d56', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_56 = df5_dct_56.pivot_table(values='invo_str3_ct_d56', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_56.columns.name = None

    # Reset the index
    df5_dct_56 = df5_dct_56.reset_index()

    # Replace 'NaN' with zero
    df5_dct_56.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 56 period
    df54 = pd.merge(df5, df5_dct_56,
                                   on='id', how='left')

    # Merging average word count with 'df54'
    df55 = pd.merge(df54, df53, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_56 = imp_features_dct_56()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_56)):
        if imp_features_list_dct_56[i] in df55.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_56[i])
            df55[imp_features_list_dct_56[i]] = 0

    # Filtering only important features 
    df56 = df55[df55.columns.intersection(imp_features_list_dct_56)]

    # Reindexing 
    df56 = df56.reindex(
        sorted(df56.columns), axis=1)

    # ----------------------------------------------------------------------
    # 56. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df56)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df57 = df56[df56.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df57


# *************************************************************************
# dct 63: Data collection
# *************************************************************************

def dct_63_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 63 dcts after date1
    sql = sql_d63_invoice_data()

    # Import as dataframe from redshift
    df58 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df58['invo_str1_d63'] = df58.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df58['invo_str2_d63'] = df58.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df58['invo_str3_d63'] = df58.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df58['invo_str4_d63'] = df58.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df58_fil = df58.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d63',
         'invo_str2_d63', 'invo_str3_d63', 'invo_str4_d63'])

    # Summing (grouping) all invoices for a 'id'
    df59 = df58_fil.groupby('id').mean()

    # Final word count table
    df60 = df59.filter(
        ['id', 'date1', 'invo_str1_d63',
         'invo_str2_d63', 'invo_str3_d63', 'invo_str4_d63'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d63_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d63_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = \
        df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 63
    df5_dct_63 = df5[['id', 'invo_str3_ct_d63', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_63 = \
        df5_dct_63.pivot_table(values='invo_str3_ct_d63', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_63.columns.name = None

    # Reset the index
    df5_dct_63 = df5_dct_63.reset_index()

    # Replace 'NaN' with zero
    df5_dct_63.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 63 period
    df61 = pd.merge(df5, df5_dct_63,
                                   on='id', how='left')

    # Merging average word count with 'df61'
    df62 = pd.merge(df61, df60, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_63 = imp_features_dct_63()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_63)):
        if imp_features_list_dct_63[i] in df62.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_63[i])
            df62[imp_features_list_dct_63[i]] = 0

    # Filtering only important features 
    df63 = df62[df62.columns.intersection(imp_features_list_dct_63)]

    # Reindexing 
    df63 = df63.reindex(
        sorted(df63.columns), axis=1)

    # ----------------------------------------------------------------------
    # 63. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df63)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df64 = df63[
            df63.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df64


# *************************************************************************
# dct 70: Data collection
# *************************************************************************

def dct_70_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 70 dcts after date1
    sql0 = sql_d70_invoice_data()

    # Import as dataframe from redshift
    df65 = pandas_gbq.invo_gbq(sql0, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df65['invo_str1_d70'] = df65.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df65['invo_str2_d70'] = df65.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df65['invo_str3_d70'] = df65.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df65['invo_str4_d70'] = df65.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df65_fil = df65.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d70',
         'invo_str2_d70', 'invo_str3_d70', 'invo_str4_d70'])

    # Summing (grouping) all invoices for a 'id'
    df66 = df65_fil.groupby('id').mean()

    # Final word count table
    df67 = df66.filter(
        ['id', 'date1', 'invo_str1_d70',
         'invo_str2_d70', 'invo_str3_d70', 'invo_str4_d70'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d70_rs_data()

    # Import as dataframe from GCP
    df5 =  pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql0 = sql_d70_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql0, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = \
        df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 70
    df5_dct_70 = df5[['id', 'invo_str3_ct_d70', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_70 = \
        df5_dct_70.pivot_table(values='invo_str3_ct_d70', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_70.columns.name = None

    # Reset the index
    df5_dct_70 = df5_dct_70.reset_index()

    # Replace 'NaN' with zero
    df5_dct_70.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 70 period
    df60 = pd.merge(df5, df5_dct_70,
                                   on='id', how='left')

    # Merging average word count with 'df60'
    df70 = pd.merge(df60, df67, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_70 = imp_features_dct_70()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_70)):
        if imp_features_list_dct_70[i] in df70.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_70[i])
            df70[imp_features_list_dct_70[i]] = 0

    # Filtering only important features 
    df80 = \
        df70[df70.columns.intersection(imp_features_list_dct_70)]

    # Reindexing 
    df80 = df80.reindex(
        sorted(df80.columns), axis=1)

    # ----------------------------------------------------------------------
    # 70. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df80)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df90 = \
        df80[
            df80.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df90


# *************************************************************************
# dct 77: Data collection
# *************************************************************************

def dct_77_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 77 dcts after date1
    sql7 = sql_d77_invoice_data()

    # Import as dataframe from redshift
    df68 = pandas_gbq.invo_gbq(sql7, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df68['invo_str1_d77'] = df68.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df68['invo_str2_d77'] = df68.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df68['invo_str3_d77'] = df68.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df68['invo_str4_d77'] = df68.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df68_fil = df68.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d77',
         'invo_str2_d77', 'invo_str3_d77', 'invo_str4_d77'])

    # Summing (grouping) all invoices for a 'id'
    df69 = df68_fil.groupby('id').mean()

    # Final word count table
    df70 = df69.filter(
        ['id', 'date1', 'invo_str1_d77',
         'invo_str2_d77', 'invo_str3_d77', 'invo_str4_d77'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql_rs_dct_77 = sql_d77_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql_rs_dct_77, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql7 = sql_d77_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql7, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 77
    df5_dct_77 = df5[['id', 'invo_str3_ct_d77', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_77 = df5_dct_77.pivot_table(values='invo_str3_ct_d77', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_77.columns.name = None

    # Reset the index
    df5_dct_77 = df5_dct_77.reset_index()

    # Replace 'NaN' with zero
    df5_dct_77.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 77 period
    df67 = pd.merge(df5, df5_dct_77,
                                   on='id', how='left')

    # Merging average word count with 'df67'
    df77 = pd.merge(df67, df70, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_77 = imp_features_dct_77()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_77)):
        if imp_features_list_dct_77[i] in df77.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_77[i])
            df77[imp_features_list_dct_77[i]] = 0

    # Filtering only important features 
    df87 = df77[df77.columns.intersection(imp_features_list_dct_77)]

    # Reindexing 
    df87 = df87.reindex(
        sorted(df87.columns), axis=1)

    # ----------------------------------------------------------------------
    # 77. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df87)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df97 = df87[df87.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df97


# *************************************************************************
# dct 84: Data collection
# *************************************************************************

def dct_84_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 84 dcts after date1
    sql = sql_d84_invoice_data()

    # Import as dataframe from redshift
    df71 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df71['invo_str1_d84'] = df71.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df71['invo_str2_d84'] = df71.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df71['invo_str3_d84'] = df71.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df71['invo_str4_d84'] = df71.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df71_fil = df71.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d84',
         'invo_str2_d84', 'invo_str3_d84', 'invo_str4_d84'])

    # Summing (grouping) all invoices for a 'id'
    df72 = df71_fil.groupby('id').mean()

    # Final word count table
    df73 = df72.filter(
        ['id', 'date1', 'invo_str1_d84',
         'invo_str2_d84', 'invo_str3_d84', 'invo_str4_d84'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d84_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d84_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 84
    df5_dct_84 = df5[['id', 'invo_str3_ct_d84', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot the dct 84 invo_str3s (Each Unique invo_str3 Become a Column)

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_84 = df5_dct_84.pivot_table(values='invo_str3_ct_d84', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_84.columns.name = None

    # Reset the index
    df5_dct_84 = df5_dct_84.reset_index()

    # Replace 'NaN' with zero
    df5_dct_84.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: , average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 84 period
    df74 = pd.merge(df5, df5_dct_84,
                                   on='id', how='left')

    # Merging average word count with 'df74'
    df75 = pd.merge(df74, df73, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_84 = imp_features_dct_84()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_84)):
        if imp_features_list_dct_84[i] in df75.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_84[i])
            df75[imp_features_list_dct_84[i]] = 0

    # Filtering only important features 
    df76 = df75[df75.columns.intersection(imp_features_list_dct_84)]

    # Reindexing 
    df76 = df76.reindex(
        sorted(df76.columns), axis=1)

    # ----------------------------------------------------------------------
    # 84. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df76)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df77 = df76[df76.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df77


# *************************************************************************
# dct 91: Data collection
# *************************************************************************

def dct_91_data(credentials, project_id):
    # ----------------------------------------------------------------------
    # 1. Import Invoice Data & Extract Avg Word Counts Features
    # ----------------------------------------------------------------------

    # SQL for importing all invoices created within 91 dcts after date1
    sql = sql_d91_invoice_data()

    # Import as dataframe from redshift
    df78 = pandas_gbq.invo_gbq(sql, project_id=project_id,
                                                          credentials=credentials)

    # Words count in invoice's invo_str1_, invo_str2_, invo_str3_, invo_str4_
    df78['invo_str1_d91'] = df78.apply(
        lambda x: words_count(x['invo_str1_']), axis=1)
    df78['invo_str2_d91'] = df78.apply(
        lambda x: words_count(x['invo_str2_']), axis=1)
    df78['invo_str3_d91'] = df78.apply(
        lambda x: words_count(x['invo_str3_']), axis=1)
    df78['invo_str4_d91'] = df78.apply(
        lambda x: words_count(x['invo_str4_']), axis=1)

    # Filters the text columns from the dataframe
    df78_fil = df78.filter(
        ['id', 'invo_id', 'date1', 'date2',
         'date3', 'dct', 'invo_str1_d91',
         'invo_str2_d91', 'invo_str3_d91', 'invo_str4_d91'])

    # Summing (grouping) all invoices for a 'id'
    df79 = df78_fil.groupby('id').mean()

    # Final word count table
    df80 = df79.filter(
        ['id', 'date1', 'invo_str1_d91',
         'invo_str2_d91', 'invo_str3_d91', 'invo_str4_d91'])

    # ----------------------------------------------------------------------
    # 2.  Features
    # ----------------------------------------------------------------------

    # SQL query 
    sql = sql_d91_rs_data()

    # Import as dataframe from GCP
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # ----------------------------------------------------------------------
    # 4. Import and Extract Features from invo_str3s Data
    # ----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    # 4.1. invo_str3 data collection from BQ
    # ---------------------------------------------------------------------- 
    # SQL for invo_str3s 
    sql = sql_d91_invo_str3()

    # Import as dataframe from bq
    df5 = pandas_gbq.invo_gbq(sql, project_id=project_id, credentials=credentials)

    # Removing whitespace from the invo_str3 strings
    # Removing row if there is 'None' the invo_str3 cell
    df5 = df5[~df5.astype(str).eq('None').any(1)]

    # Replace the 'NaN' cell by zero
    df5.fillna(0, inplace=True)

    # Using lambda function to remove the white space in the invo_str3 string name
    df5['invo_str3_nm'] = \
        df5.apply(lambda x: x['invo_str3'].replace(' ', '').replace('-', '').replace('/', ''), axis=1)

    # Filtered the invo_str3s columns for dct 91
    df5_dct_91 = df5[['id', 'invo_str3_ct', 'invo_str3_nm']]

    # ----------------------------------------------------------------------
    # 4.2. Pivot the invo_str3s (each unique invo_str3 become a column)
    # ----------------------------------------------------------------------

    # Pivot the dct 91 invo_str3s (Each Unique invo_str3 Become a Column)

    # Pivot table based on the unique column value in 'invo_str3_nm'
    df5_dct_91 = df5_dct_91.pivot_table(values='invo_str3_ct', columns='invo_str3_nm', index='id',
                                                  aggfunc=np.sum, fill_value=0)

    # Drop the old column name
    df5_dct_91.columns.name = None

    # Reset the index
    df5_dct_91 = df5_dct_91.reset_index()

    # Replace 'NaN' with zero
    df5_dct_91.fillna(0, inplace=True)

    # ----------------------------------------------------------------------
    # 5. Merging all data: average word count and invo_str3 data
    # ----------------------------------------------------------------------
    # Merging  and invo_str3s data for dct 91 period
    df81 = pd.merge(df5, df5_dct_91,
                                   on='id', how='left')

    # Merging average word count with 'df81'
    df82 = pd.merge(df81, df80, on='id',
                                          how='left')

    # ----------------------------------------------------------------------
    # 6. Filtering only important features
    # ----------------------------------------------------------------------
    # Importing importing features list
    imp_features_list_dct_91 = imp_features_dct_91()

    # invo_str4_ding missing important feature column with zero values (if there any!)
    for i in range(len(imp_features_list_dct_91)):
        if imp_features_list_dct_91[i] in df82.columns:
            continue
        else:
            # print("False: ", imp_features_list_dct_91[i])
            df82[imp_features_list_dct_91[i]] = 0

    # Filtering only important features 
    df83 = df82[df82.columns.intersection(imp_features_list_dct_91)]

    # Reindexing 
    df83 = df83.reindex(
        sorted(df83.columns), axis=1)

    # ----------------------------------------------------------------------
    # 91. Filtering inactive users' acc
    # ----------------------------------------------------------------------

    # Excluded columns for checking
    ex_cols_list = ['invo_str5', 'invo_fes1', 'invo_fes2', 'date1', 'id']
    cols_list = list(df83)
    cols = list(set(cols_list) - set(ex_cols_list))

    # Filtering out all inactive users acc
    df84 = df83[df83.apply(lambda x: cell_value_sum(x, cols) > 0, axis=1)]

    # ----------------------------------------------------------------------
    # 8. Returning the filtered features data for new acc
    # ----------------------------------------------------------------------

    return df84
