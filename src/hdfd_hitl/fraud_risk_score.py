
# -----------------------------------
# Standard library imports
# -----------------------------------
from __builtin__ import sorted, print
from datetime import date
import pandas as pd


# -----------------------------------------
# Internal library imports (util functions)
# -----------------------------------------
from src import data_standardization, fit_model, data_collection_from_gcp

# -----------------------------------------
# GCP credentials
# -----------------------------------------
from google.oauth2 import service_account

from src.data_collection_from_gcp import *

credentials = service_account.Credentials.from_service_account_file()

project_id = ""


def prioritized_fra(credentials_, project_id_, n):

    # GCP Credential and project id
    credentials = credentials_
    project_id = project_id_

    # -----------------------------------------
    # Import New Users' Accounts From BQ
    # -----------------------------------------

    # Day 7
    df7 = day_7_data(credentials, project_id)
    df7 = df7.dropna()          # Drop rows with NaN
    df7_sort = df7.reindex(sorted(df7.columns), axis=1)   # Sorting columns
    df7_ana = df7_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 14
    df14 = day_14_data(credentials, project_id)
    df14 = df14.dropna()          # Drop rows with NaN
    df14_sort = df14.reindex(sorted(df14.columns), axis=1)   # Sorting columns
    df14_ana = df14_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 21
    df21 = day_21_data(credentials, project_id)
    df21 = df21.dropna()          # Drop rows with NaN
    df21_sort = df21.reindex(sorted(df21.columns), axis=1)   # Sorting columns
    df21_ana = df21_sort.copy()   # Create a copy of the dataframe for

    # Day 28
    df28 = day_28_data(credentials, project_id)
    df28 = df28.dropna()          # Drop rows with NaN
    df28_sort = df28.reindex(sorted(df28.columns), axis=1)   # Sorting columns
    df28_ana = df28_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 35
    df35 = day_35_data(credentials, project_id)
    df35 = df35.dropna()          # Drop rows with NaN
    df35_sort = df35.reindex(sorted(df35.columns), axis=1)   # Sorting columns
    df35_ana = df35_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 42
    df42 = day_42_data(credentials, project_id)
    df42 = df42.dropna()          # Drop rows with NaN
    df42_sort = df42.reindex(sorted(df42.columns), axis=1)   # Sorting columns
    df42_ana = df42_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 49
    df49 = day_49_data(credentials, project_id)
    df49 = df49.dropna()          # Drop rows with NaN
    df49_sort = df49.reindex(sorted(df49.columns), axis=1)   # Sorting columns
    df49_ana = df49_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 56
    df56 = day_56_data(credentials, project_id)
    df56 = df56.dropna()          # Drop rows with NaN
    df56_sort = df56.reindex(sorted(df56.columns), axis=1)   # Sorting columns
    df56_ana = df56_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 63
    df63 = day_63_data(credentials, project_id)
    df63 = df63.dropna()          # Drop rows with NaN
    df63_sort = df63.reindex(sorted(df63.columns), axis=1)   # Sorting columns
    df63_ana = df63_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 63
    df63 = day_63_data(credentials, project_id)
    df63 = df63.dropna()          # Drop rows with NaN
    df63_sort = df63.reindex(sorted(df63.columns), axis=1)   # Sorting columns
    df63_ana = df63_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 70
    df70 = day_70_data(credentials, project_id)
    df70 = df70.dropna()          # Drop rows with NaN
    df70_sort = df70.reindex(sorted(df70.columns), axis=1)   # Sorting columns
    df70_ana = df70_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 77
    df77 = day_77_data(credentials, project_id)
    df77 = df77.dropna()          # Drop rows with NaN
    df77_sort = df77.reindex(sorted(df77.columns), axis=1)   # Sorting columns
    df77_ana = df77_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 84
    df84 = day_84_data(credentials, project_id)
    df84 = df84.dropna()          # Drop rows with NaN
    df84_sort = df84.reindex(sorted(df84.columns), axis=1)   # Sorting columns
    df84_ana = df84_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 91
    df91 = day_91_data(credentials, project_id)
    df91 = df91.dropna()          # Drop rows with NaN
    df91_sort = df91.reindex(sorted(df91.columns), axis=1)   # Sorting columns
    df91_ana = df91_sort.copy()   # Create a copy of the dataframe for analysis

    print("Data collection finished!")

    # --------------------------------------------------------------------
    # Data Standardization
    # --------------------------------------------------------------------
    df7_scaled = data_standardization.min_max_stnd(df7_ana, 7)		# Day 7
    df14_scaled = data_standardization.min_max_stnd(df14_ana, 14)		# Day 14
    df21_scaled = data_standardization.min_max_stnd(df21_ana, 21)		# Day 21
    df28_scaled = data_standardization.min_max_stnd(df28_ana, 28)		# Day 28
    df35_scaled = data_standardization.min_max_stnd(df35_ana, 35)		# Day 35
    df42_scaled = data_standardization.min_max_stnd(df42_ana, 42)		# Day 42
    df49_scaled = data_standardization.min_max_stnd(df49_ana, 49)		# Day 49
    df56_scaled = data_standardization.min_max_stnd(df56_ana, 56)		# Day 56
    df63_scaled = data_standardization.min_max_stnd(df63_ana, 63)		# Day 63
    df70_scaled = data_standardization.min_max_stnd(df70_ana, 70)		# Day 70
    df77_scaled = data_standardization.min_max_stnd(df77_ana, 77)		# Day 77
    df84_scaled = data_standardization.min_max_stnd(df84_ana, 84)		# Day 84
    df91_scaled = data_standardization.min_max_stnd(df91_ana, 91)		# Day 91

    print("Data standardization finished!")

    # --------------------------------------------------------------------
    # GMM Clustering
    # --------------------------------------------------------------------
    df7_gmm_frc = fit_model.unsupervised_gmm(df7_ana, df7_scaled, 7)         # Day 7
    df14_gmm_frc = fit_model.unsupervised_gmm(df14_ana, df14_scaled, 14)     # Day 14
    df21_gmm_frc = fit_model.unsupervised_gmm(df21_ana, df21_scaled, 21)     # Day 21
    df28_gmm_frc = fit_model.unsupervised_gmm(df28_ana, df28_scaled, 28)     # Day 28
    df35_gmm_frc = fit_model.unsupervised_gmm(df35_ana, df35_scaled, 35)     # Day 35
    df42_gmm_frc = fit_model.unsupervised_gmm(df42_ana, df42_scaled, 42)     # Day 42
    df49_gmm_frc = fit_model.unsupervised_gmm(df49_ana, df49_scaled, 49)     # Day 49
    df56_gmm_frc = fit_model.unsupervised_gmm(df56_ana, df56_scaled, 56)     # Day 56
    df63_gmm_frc = fit_model.unsupervised_gmm(df63_ana, df63_scaled, 63)     # Day 63
    df70_gmm_frc = fit_model.unsupervised_gmm(df70_ana, df70_scaled, 70)     # Day 70
    df77_gmm_frc = fit_model.unsupervised_gmm(df77_ana, df77_scaled, 77)     # Day 77
    df84_gmm_frc = fit_model.unsupervised_gmm(df84_ana, df84_scaled, 84)     # Day 84
    df91_gmm_frc = fit_model.unsupervised_gmm(df91_ana, df91_scaled, 91)     # Day 91

    print("GMM clustering finished!")

    # --------------------------------------------------------------------
    # NN Classifier
    # --------------------------------------------------------------------
    df7_nn_frc = fit_model.nn_classifier(df7_ana, df7_scaled, 14)        # Day 7
    df14_nn_frc = fit_model.nn_classifier(df14_ana, df14_scaled, 14)     # Day 14
    df21_nn_frc = fit_model.nn_classifier(df21_ana, df21_scaled, 21)     # Day 21
    df28_nn_frc = fit_model.nn_classifier(df28_ana, df28_scaled, 14)     # Day 28
    df35_nn_frc = fit_model.nn_classifier(df35_ana, df35_scaled, 14)     # Day 35
    df42_nn_frc = fit_model.nn_classifier(df42_ana, df42_scaled, 14)     # Day 42
    df49_nn_frc = fit_model.nn_classifier(df49_ana, df49_scaled, 14)     # Day 49
    df56_nn_frc = fit_model.nn_classifier(df56_ana, df56_scaled, 14)     # Day 56
    df63_nn_frc = fit_model.nn_classifier(df63_ana, df63_scaled, 14)     # Day 63
    df70_nn_frc = fit_model.nn_classifier(df70_ana, df70_scaled, 14)     # Day 70
    df77_nn_frc = fit_model.nn_classifier(df77_ana, df77_scaled, 14)     # Day 77
    df84_nn_frc = fit_model.nn_classifier(df84_ana, df84_scaled, 14)     # Day 84
    df91_nn_frc = fit_model.nn_classifier(df91_ana, df91_scaled, 14)     # Day 91

    print("NN classifier finished!")

    # --------------------------------------------------------------------
    # Merging Weekly GMM and NN Fraud Risk Cluster (FRC) based on 'id'
    # --------------------------------------------------------------------
    df7_nn_frc = pd.DataFrame(df7_nn_frc['id'])
    df7_frc = pd.merge(df7_gmm_frc, df7_nn_frc, how='inner', on=['id'])

    df14_nn_frc = pd.DataFrame(df14_nn_frc['id'])
    df14_frc = pd.merge(df14_gmm_frc, df14_nn_frc, how='inner', on=['id'])

    df21_nn_frc = pd.DataFrame(df21_nn_frc['id'])
    df21_frc = pd.merge(df21_gmm_frc, df21_nn_frc, how='inner', on=['id'])

    df28_nn_frc = pd.DataFrame(df28_nn_frc['id'])
    df28_frc = pd.merge(df28_gmm_frc, df28_nn_frc, how='inner', on=['id'])

    df35_nn_frc = pd.DataFrame(df35_nn_frc['id'])
    df35_frc = pd.merge(df35_gmm_frc, df35_nn_frc, how='inner', on=['id'])

    df42_nn_frc = pd.DataFrame(df42_nn_frc['id'])
    df42_frc = pd.merge(df42_gmm_frc, df42_nn_frc, how='inner', on=['id'])

    df49_nn_frc = pd.DataFrame(df49_nn_frc['id'])
    df49_frc = pd.merge(df49_gmm_frc, df49_nn_frc, how='inner', on=['id'])

    df56_nn_frc = pd.DataFrame(df56_nn_frc['id'])
    df56_frc = pd.merge(df56_gmm_frc, df56_nn_frc, how='inner', on=['id'])

    df63_nn_frc = pd.DataFrame(df63_nn_frc['id'])
    df63_frc = pd.merge(df63_gmm_frc, df63_nn_frc, how='inner', on=['id'])

    df70_nn_frc = pd.DataFrame(df70_nn_frc['id'])
    df70_frc = pd.merge(df70_gmm_frc, df70_nn_frc, how='inner', on=['id'])

    df77_nn_frc = pd.DataFrame(df77_nn_frc['id'])
    df77_frc = pd.merge(df77_gmm_frc, df77_nn_frc, how='inner', on=['id'])

    df84_nn_frc = pd.DataFrame(df84_nn_frc['id'])
    df84_frc = pd.merge(df84_gmm_frc, df84_nn_frc, how='inner', on=['id'])

    df91_nn_frc = pd.DataFrame(df91_nn_frc['id'])
    df91_frc = pd.merge(df91_gmm_frc, df91_nn_frc, how='inner', on=['id'])

    print("Merging Weekly Segmented FRCs finished!")

    # --------------------------------------------------------------------
    # Adding Weekly Segmented Model Name Column
    # --------------------------------------------------------------------
    df7_frc['model_name'] = 'D07'
    df14_frc['model_name'] = 'D14'
    df21_frc['model_name'] = 'D21'
    df28_frc['model_name'] = 'D28'
    df35_frc['model_name'] = 'D35'
    df42_frc['model_name'] = 'D42'
    df49_frc['model_name'] = 'D49'
    df56_frc['model_name'] = 'D56'
    df63_frc['model_name'] = 'D63'
    df70_frc['model_name'] = 'D70'
    df77_frc['model_name'] = 'D77'
    df84_frc['model_name'] = 'D84'
    df91_frc['model_name'] = 'D91'

    print("Weekly Segmented Model Column Added!")

    # --------------------------------------------------------------------
    # Merging All Weekly Segmented Clusters Together
    # --------------------------------------------------------------------
    df_merge_frc = df7_frc.copy()
    df_merge_frc = df_merge_frc.append(df14_frc)
    df_merge_frc = df_merge_frc.append(df21_frc)
    df_merge_frc = df_merge_frc.append(df28_frc)
    df_merge_frc = df_merge_frc.append(df35_frc)
    df_merge_frc = df_merge_frc.append(df42_frc)
    df_merge_frc = df_merge_frc.append(df49_frc)
    df_merge_frc = df_merge_frc.append(df56_frc)
    df_merge_frc = df_merge_frc.append(df63_frc)
    df_merge_frc = df_merge_frc.append(df70_frc)
    df_merge_frc = df_merge_frc.append(df77_frc)
    df_merge_frc = df_merge_frc.append(df84_frc)
    df_merge_frc = df_merge_frc.append(df91_frc)

    # Reindexing the merged dataframe
    df_merge_frc = df_merge_frc.reset_index(drop=True)

    print("Merging all weekly segmented model is finished!")

    # --------------------------------------------------------------------
    # Computing Fraud Risk Score (FRS)
    # --------------------------------------------------------------------

    # Calculating Fraud Risk Score (FRS) for each user account
    col_list = ['days_on_platform', 'declinedonlinepaymentnotification', 'emailinvoice', 'invoice_count']
    fraud_risk_score_ = df_merge_frc.apply(lambda x: fraud_risk_score(x), axis=1)

    # Adding Fraud Risk Score (FRS) column
    df_merge_frc['fraud_risk_score'] = fraud_risk_score_    # adding this fraud risk score as a column in the dataframe

    print('Computing Fraud Risk Score is finished')

    # --------------------------------------------------------------------
    # Remove already labeled Fraud Risk Account (FRA)
    # --------------------------------------------------------------------

    # Import labeled fraud risk accounts (labeled by support team)
    df_labeled_fraud_id = labeled_fraud_data(credentials, project_id)

    # Cross match users accounts, if any account is already labeled then remove it
    df_merge_frc_and_labeled = pd.merge(df_merge_frc, df_labeled_fraud_id,
                                        how='left', on=['id'], indicator=True)
    df_merge_frc_not_labeled = df_merge_frc_and_labeled[df_merge_frc_and_labeled._merge != 'both']
    df_merge_frc_not_labeled = df_merge_frc_not_labeled.drop(columns=['_merge'], axis=1)

    print('Removed already labeled FRA is finished')

    # --------------------------------------------------------------------
    # Sorting User Account Based on FRS
    # --------------------------------------------------------------------

    # Sorting and reindexing report data frame based on the 'fraud_risk_score'
    df_merge_frc_not_labeled.sort_values('fraud_risk_score', axis=0, ascending=False, inplace=True, na_position='last')

    # Reindexing the sorted dataframe
    df_merge_frc_not_labeled = df_merge_frc_not_labeled.reset_index(drop=True)

    print('Sorting user account based on FRS')

    # --------------------------------------------------------------------
    # Reporting: HUMAN-IN-THE-LOOP - To Be Labeled (TBL) by HUMAN (Only Top N)
    # --------------------------------------------------------------------

    # Adding effective date and model name column
    df_merge_frc_not_labeled['fraud_label'] = -1  # To Be Labeled (TBL) by support team
    df_merge_frc_not_labeled['support_note'] = 'NaN'  # To Be Labeled (TBL) by support team

    # Top N accounts for support team reporting
    df_fra_top_N_all_features = df_merge_frc_not_labeled.head(n)

    # Selecting columns for support team reporting
    df_fra_top_N_for_support = df_fra_top_N_all_features[['id', 'invo_fea1', 'invo_fea2',
                                                           'date1', 'invo_fea3',
                                                           'fraud_label', 'support_note']]

    print('Return final top N FRA')
    return df_fra_top_N_for_support
