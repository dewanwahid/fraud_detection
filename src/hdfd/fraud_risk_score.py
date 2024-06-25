
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

credentials = service_account.Credentials.from_service_account_file(
    '', )

project_id = ""


def prioritized_fra(credentials_, project_id_, n):

    # GCP Credential and project id
    credentials = credentials_
    project_id = project_id_

    # -----------------------------------------
    # Import New Users' Accounts From BQ
    # -----------------------------------------

    # Day 7
    df_day_7 = day_7_data(credentials, project_id)
    df_day_7 = df_day_7.dropna()          # Drop rows with NaN
    df_day_7_sort = df_day_7.reindex(sorted(df_day_7.columns), axis=1)   # Sorting columns
    df_day_7_ana = df_day_7_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 14
    df_day_14 = day_14_data(credentials, project_id)
    df_day_14 = df_day_14.dropna()          # Drop rows with NaN
    df_day_14_sort = df_day_14.reindex(sorted(df_day_14.columns), axis=1)   # Sorting columns
    df_day_14_ana = df_day_14_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 21
    df_day_21 = day_21_data(credentials, project_id)
    df_day_21 = df_day_21.dropna()          # Drop rows with NaN
    df_day_21_sort = df_day_21.reindex(sorted(df_day_21.columns), axis=1)   # Sorting columns
    df_day_21_ana = df_day_21_sort.copy()   # Create a copy of the dataframe for

    # Day 28
    df_day_28 = day_28_data(credentials, project_id)
    df_day_28 = df_day_28.dropna()          # Drop rows with NaN
    df_day_28_sort = df_day_28.reindex(sorted(df_day_28.columns), axis=1)   # Sorting columns
    df_day_28_ana = df_day_28_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 35
    df_day_35 = day_35_data(credentials, project_id)
    df_day_35 = df_day_35.dropna()          # Drop rows with NaN
    df_day_35_sort = df_day_35.reindex(sorted(df_day_35.columns), axis=1)   # Sorting columns
    df_day_35_ana = df_day_35_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 42
    df_day_42 = day_42_data(credentials, project_id)
    df_day_42 = df_day_42.dropna()          # Drop rows with NaN
    df_day_42_sort = df_day_42.reindex(sorted(df_day_42.columns), axis=1)   # Sorting columns
    df_day_42_ana = df_day_42_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 49
    df_day_49 = day_49_data(credentials, project_id)
    df_day_49 = df_day_49.dropna()          # Drop rows with NaN
    df_day_49_sort = df_day_49.reindex(sorted(df_day_49.columns), axis=1)   # Sorting columns
    df_day_49_ana = df_day_49_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 56
    df_day_56 = day_56_data(credentials, project_id)
    df_day_56 = df_day_56.dropna()          # Drop rows with NaN
    df_day_56_sort = df_day_56.reindex(sorted(df_day_56.columns), axis=1)   # Sorting columns
    df_day_56_ana = df_day_56_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 63
    df_day_63 = day_63_data(credentials, project_id)
    df_day_63 = df_day_63.dropna()          # Drop rows with NaN
    df_day_63_sort = df_day_63.reindex(sorted(df_day_63.columns), axis=1)   # Sorting columns
    df_day_63_ana = df_day_63_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 63
    df_day_63 = day_63_data(credentials, project_id)
    df_day_63 = df_day_63.dropna()          # Drop rows with NaN
    df_day_63_sort = df_day_63.reindex(sorted(df_day_63.columns), axis=1)   # Sorting columns
    df_day_63_ana = df_day_63_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 70
    df_day_70 = day_70_data(credentials, project_id)
    df_day_70 = df_day_70.dropna()          # Drop rows with NaN
    df_day_70_sort = df_day_70.reindex(sorted(df_day_70.columns), axis=1)   # Sorting columns
    df_day_70_ana = df_day_70_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 77
    df_day_77 = day_77_data(credentials, project_id)
    df_day_77 = df_day_77.dropna()          # Drop rows with NaN
    df_day_77_sort = df_day_77.reindex(sorted(df_day_77.columns), axis=1)   # Sorting columns
    df_day_77_ana = df_day_77_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 84
    df_day_84 = day_84_data(credentials, project_id)
    df_day_84 = df_day_84.dropna()          # Drop rows with NaN
    df_day_84_sort = df_day_84.reindex(sorted(df_day_84.columns), axis=1)   # Sorting columns
    df_day_84_ana = df_day_84_sort.copy()   # Create a copy of the dataframe for analysis

    # Day 91
    df_day_91 = day_91_data(credentials, project_id)
    df_day_91 = df_day_91.dropna()          # Drop rows with NaN
    df_day_91_sort = df_day_91.reindex(sorted(df_day_91.columns), axis=1)   # Sorting columns
    df_day_91_ana = df_day_91_sort.copy()   # Create a copy of the dataframe for analysis

    print("Data collection finished!")

    # --------------------------------------------------------------------
    # Data Standardization
    # --------------------------------------------------------------------
    df_day_7_scaled = data_standardization.min_max_stnd(df_day_7_ana, 7)		# Day 7
    df_day_14_scaled = data_standardization.min_max_stnd(df_day_14_ana, 14)		# Day 14
    df_day_21_scaled = data_standardization.min_max_stnd(df_day_21_ana, 21)		# Day 21
    df_day_28_scaled = data_standardization.min_max_stnd(df_day_28_ana, 28)		# Day 28
    df_day_35_scaled = data_standardization.min_max_stnd(df_day_35_ana, 35)		# Day 35
    df_day_42_scaled = data_standardization.min_max_stnd(df_day_42_ana, 42)		# Day 42
    df_day_49_scaled = data_standardization.min_max_stnd(df_day_49_ana, 49)		# Day 49
    df_day_56_scaled = data_standardization.min_max_stnd(df_day_56_ana, 56)		# Day 56
    df_day_63_scaled = data_standardization.min_max_stnd(df_day_63_ana, 63)		# Day 63
    df_day_70_scaled = data_standardization.min_max_stnd(df_day_70_ana, 70)		# Day 70
    df_day_77_scaled = data_standardization.min_max_stnd(df_day_77_ana, 77)		# Day 77
    df_day_84_scaled = data_standardization.min_max_stnd(df_day_84_ana, 84)		# Day 84
    df_day_91_scaled = data_standardization.min_max_stnd(df_day_91_ana, 91)		# Day 91

    print("Data standardization finished!")

    # --------------------------------------------------------------------
    # GMM Clustering
    # --------------------------------------------------------------------
    df_day_7_gmm_frc = fit_model.unsupervised_gmm(df_day_7_ana, df_day_7_scaled, 7)         # Day 7
    df_day_14_gmm_frc = fit_model.unsupervised_gmm(df_day_14_ana, df_day_14_scaled, 14)     # Day 14
    df_day_21_gmm_frc = fit_model.unsupervised_gmm(df_day_21_ana, df_day_21_scaled, 21)     # Day 21
    df_day_28_gmm_frc = fit_model.unsupervised_gmm(df_day_28_ana, df_day_28_scaled, 28)     # Day 28
    df_day_35_gmm_frc = fit_model.unsupervised_gmm(df_day_35_ana, df_day_35_scaled, 35)     # Day 35
    df_day_42_gmm_frc = fit_model.unsupervised_gmm(df_day_42_ana, df_day_42_scaled, 42)     # Day 42
    df_day_49_gmm_frc = fit_model.unsupervised_gmm(df_day_49_ana, df_day_49_scaled, 49)     # Day 49
    df_day_56_gmm_frc = fit_model.unsupervised_gmm(df_day_56_ana, df_day_56_scaled, 56)     # Day 56
    df_day_63_gmm_frc = fit_model.unsupervised_gmm(df_day_63_ana, df_day_63_scaled, 63)     # Day 63
    df_day_70_gmm_frc = fit_model.unsupervised_gmm(df_day_70_ana, df_day_70_scaled, 70)     # Day 70
    df_day_77_gmm_frc = fit_model.unsupervised_gmm(df_day_77_ana, df_day_77_scaled, 77)     # Day 77
    df_day_84_gmm_frc = fit_model.unsupervised_gmm(df_day_84_ana, df_day_84_scaled, 84)     # Day 84
    df_day_91_gmm_frc = fit_model.unsupervised_gmm(df_day_91_ana, df_day_91_scaled, 91)     # Day 91

    print("GMM clustering finished!")

    # --------------------------------------------------------------------
    # NN Classifier
    # --------------------------------------------------------------------
    df_day_7_nn_frc = fit_model.nn_classifier(df_day_7_ana, df_day_7_scaled, 14)        # Day 7
    df_day_14_nn_frc = fit_model.nn_classifier(df_day_14_ana, df_day_14_scaled, 14)     # Day 14
    df_day_21_nn_frc = fit_model.nn_classifier(df_day_21_ana, df_day_21_scaled, 21)     # Day 21
    df_day_28_nn_frc = fit_model.nn_classifier(df_day_28_ana, df_day_28_scaled, 14)     # Day 28
    df_day_35_nn_frc = fit_model.nn_classifier(df_day_35_ana, df_day_35_scaled, 14)     # Day 35
    df_day_42_nn_frc = fit_model.nn_classifier(df_day_42_ana, df_day_42_scaled, 14)     # Day 42
    df_day_49_nn_frc = fit_model.nn_classifier(df_day_49_ana, df_day_49_scaled, 14)     # Day 49
    df_day_56_nn_frc = fit_model.nn_classifier(df_day_56_ana, df_day_56_scaled, 14)     # Day 56
    df_day_63_nn_frc = fit_model.nn_classifier(df_day_63_ana, df_day_63_scaled, 14)     # Day 63
    df_day_70_nn_frc = fit_model.nn_classifier(df_day_70_ana, df_day_70_scaled, 14)     # Day 70
    df_day_77_nn_frc = fit_model.nn_classifier(df_day_77_ana, df_day_77_scaled, 14)     # Day 77
    df_day_84_nn_frc = fit_model.nn_classifier(df_day_84_ana, df_day_84_scaled, 14)     # Day 84
    df_day_91_nn_frc = fit_model.nn_classifier(df_day_91_ana, df_day_91_scaled, 14)     # Day 91

    print("NN classifier finished!")

    # --------------------------------------------------------------------
    # Merging Weekly GMM and NN Fraud Risk Cluster (FRC) based on 'id'
    # --------------------------------------------------------------------
    df_day_7_nn_frc_sysid = pd.DataFrame(df_day_7_nn_frc['id'])
    df_day_7_frc = pd.merge(df_day_7_gmm_frc, df_day_7_nn_frc_sysid, how='inner', on=['id'])

    df_day_14_nn_frc_sysid = pd.DataFrame(df_day_14_nn_frc['id'])
    df_day_14_frc = pd.merge(df_day_14_gmm_frc, df_day_14_nn_frc_sysid, how='inner', on=['id'])

    df_day_21_nn_frc_sysid = pd.DataFrame(df_day_21_nn_frc['id'])
    df_day_21_frc = pd.merge(df_day_21_gmm_frc, df_day_21_nn_frc_sysid, how='inner', on=['id'])

    df_day_28_nn_frc_sysid = pd.DataFrame(df_day_28_nn_frc['id'])
    df_day_28_frc = pd.merge(df_day_28_gmm_frc, df_day_28_nn_frc_sysid, how='inner', on=['id'])

    df_day_35_nn_frc_sysid = pd.DataFrame(df_day_35_nn_frc['id'])
    df_day_35_frc = pd.merge(df_day_35_gmm_frc, df_day_35_nn_frc_sysid, how='inner', on=['id'])

    df_day_42_nn_frc_sysid = pd.DataFrame(df_day_42_nn_frc['id'])
    df_day_42_frc = pd.merge(df_day_42_gmm_frc, df_day_42_nn_frc_sysid, how='inner', on=['id'])

    df_day_49_nn_frc_sysid = pd.DataFrame(df_day_49_nn_frc['id'])
    df_day_49_frc = pd.merge(df_day_49_gmm_frc, df_day_49_nn_frc_sysid, how='inner', on=['id'])

    df_day_56_nn_frc_sysid = pd.DataFrame(df_day_56_nn_frc['id'])
    df_day_56_frc = pd.merge(df_day_56_gmm_frc, df_day_56_nn_frc_sysid, how='inner', on=['id'])

    df_day_63_nn_frc_sysid = pd.DataFrame(df_day_63_nn_frc['id'])
    df_day_63_frc = pd.merge(df_day_63_gmm_frc, df_day_63_nn_frc_sysid, how='inner', on=['id'])

    df_day_70_nn_frc_sysid = pd.DataFrame(df_day_70_nn_frc['id'])
    df_day_70_frc = pd.merge(df_day_70_gmm_frc, df_day_70_nn_frc_sysid, how='inner', on=['id'])

    df_day_77_nn_frc_sysid = pd.DataFrame(df_day_77_nn_frc['id'])
    df_day_77_frc = pd.merge(df_day_77_gmm_frc, df_day_77_nn_frc_sysid, how='inner', on=['id'])

    df_day_84_nn_frc_sysid = pd.DataFrame(df_day_84_nn_frc['id'])
    df_day_84_frc = pd.merge(df_day_84_gmm_frc, df_day_84_nn_frc_sysid, how='inner', on=['id'])

    df_day_91_nn_frc_sysid = pd.DataFrame(df_day_91_nn_frc['id'])
    df_day_91_frc = pd.merge(df_day_91_gmm_frc, df_day_91_nn_frc_sysid, how='inner', on=['id'])

    print("Merging Weekly Segmented FRCs finished!")

    # --------------------------------------------------------------------
    # Adding Weekly Segmented Model Name Column
    # --------------------------------------------------------------------
    df_day_7_frc['model_name'] = 'D07'
    df_day_14_frc['model_name'] = 'D14'
    df_day_21_frc['model_name'] = 'D21'
    df_day_28_frc['model_name'] = 'D28'
    df_day_35_frc['model_name'] = 'D35'
    df_day_42_frc['model_name'] = 'D42'
    df_day_49_frc['model_name'] = 'D49'
    df_day_56_frc['model_name'] = 'D56'
    df_day_63_frc['model_name'] = 'D63'
    df_day_70_frc['model_name'] = 'D70'
    df_day_77_frc['model_name'] = 'D77'
    df_day_84_frc['model_name'] = 'D84'
    df_day_91_frc['model_name'] = 'D91'

    print("Weekly Segmented Model Column Added!")

    # --------------------------------------------------------------------
    # Merging All Weekly Segmented Clusters Together
    # --------------------------------------------------------------------
    df_merge_frc = df_day_7_frc.copy()
    df_merge_frc = df_merge_frc.append(df_day_14_frc)
    df_merge_frc = df_merge_frc.append(df_day_21_frc)
    df_merge_frc = df_merge_frc.append(df_day_28_frc)
    df_merge_frc = df_merge_frc.append(df_day_35_frc)
    df_merge_frc = df_merge_frc.append(df_day_42_frc)
    df_merge_frc = df_merge_frc.append(df_day_49_frc)
    df_merge_frc = df_merge_frc.append(df_day_56_frc)
    df_merge_frc = df_merge_frc.append(df_day_63_frc)
    df_merge_frc = df_merge_frc.append(df_day_70_frc)
    df_merge_frc = df_merge_frc.append(df_day_77_frc)
    df_merge_frc = df_merge_frc.append(df_day_84_frc)
    df_merge_frc = df_merge_frc.append(df_day_91_frc)

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
    # Reporting: To Be Labeled (TBL) by Support Team (Only Top N)
    # --------------------------------------------------------------------

    # Adding effective date and model name column
    df_merge_frc_not_labeled['fraud_label'] = -1  # To Be Labeled (TBL) by support team
    df_merge_frc_not_labeled['support_note'] = 'NaN'  # To Be Labeled (TBL) by support team

    # Top N accounts for support team reporting
    df_fra_top_N_all_features = df_merge_frc_not_labeled.head(n)

    # Selecting columns for support team reporting
    df_fra_top_N_for_support = df_fra_top_N_all_features[['id', 'admin_email', 'signup_date',
                                                           'effective_date', 'days_on_platform',
                                                           'fraud_label', 'support_note']]

    print('Return final top N FRA')
    return df_fra_top_N_for_support
