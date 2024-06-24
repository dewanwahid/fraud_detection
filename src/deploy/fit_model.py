# ---------------------------------------
# Standard library imports
# ---------------------------------------
import pickle


# -----------------------------------------------
# Fitting unsupervised GMM model
# ------------------------------------------------
def unsupervised_gmm(df_original, df_scaled, day):
    # GMM
    path0 = '/Users/dewanferdouswahid/PycharmProjects/fraud_deploy/saved_models/'
    path_ = ''
    frd_cluster_id = -1

    if day == 7:
        path_ = path0 + 'fdm_gmm_day_07_k7_model.sav'
        frd_cluster_id = 1
    elif day == 14:
        path_ = path0 + 'fdm_gmm_day_14_k8_model.sav'
        frd_cluster_id = 1
    elif day == 21:
        path_ = path0 + 'fdm_gmm_day_21_k6_model.sav'
        frd_cluster_id = 5
    elif day == 28:
        path_ = path0 + 'fdm_gmm_day_28_k6_model.sav'
        frd_cluster_id = 5
    elif day == 35:
        path_ = path0 + 'fdm_gmm_day_35_k8_model.sav'
        frd_cluster_id = 5
    elif day == 42:
        path_ = path0 + 'fdm_gmm_day_42_k7_model.sav'
        frd_cluster_id = 1
    elif day == 49:
        path_ = path0 + 'fdm_gmm_day_49_k6_model.sav'
        frd_cluster_id = 2
    elif day == 56:
        path_ = path0 + 'fdm_gmm_day_56_k8_model.sav'
        frd_cluster_id = 5
    elif day == 63:
        path_ = path0 + 'fdm_gmm_day_63_k6_model.sav'
        frd_cluster_id = 5
    elif day == 70:
        path_ = path0 + 'fdm_gmm_day_70_k8_model.sav'
        frd_cluster_id = 7
    elif day == 77:
        path_ = path0 + 'fdm_gmm_day_77_k6_model.sav'
        frd_cluster_id = 1
    elif day == 84:
        path_ = path0 + 'fdm_gmm_day_84_k8_model.sav'
        frd_cluster_id = 1
    elif day == 91:
        path_ = path0 + 'fdm_gmm_day_91_k7_model.sav'
        frd_cluster_id = 5
    else:
        print ("Enter correct model day!")

    # -------------------------------------------------------
    # Load and fit GMM model
    # -------------------------------------------------------

    # Load the saved gmm model
    gmm_model_file = open(path_, 'rb')
    gmm_model = pickle.load(gmm_model_file)
    gmm_model_file.close()

    # Predicting clustering
    gmm_cluster_id = gmm_model.predict(df_scaled)

    # Adding clusters id of each account to the dataframe
    df_original['gmm_cluster_id'] = gmm_cluster_id

    # -------------------------------------------------------
    # Filtering users accounts associated with fraud clusters
    # -------------------------------------------------------

    print("FRC ID: ", frd_cluster_id)

    # The only the accounts labeled as cluster c01 (fraud cluster)
    df_gmm_fraud_cluster = df_original[df_original.gmm_cluster_id == frd_cluster_id]

    df_gmm_fraud_cluster_ = df_gmm_fraud_cluster.reset_index()  # resetting index
    df_gmm_fraud_cluster_ = df_gmm_fraud_cluster_.drop(columns=['index'], axis=1)  # drop added columns during reindex

    # df_gmm_fraud_cluster_.to_csv(
    #     '/Users/dewanferdouswahid/PycharmProjects/fraud_deploy/test/day_14_frc_inside_ndrp_gmm.csv')

    # Dropping the cluster and class id label columns
    df_gmm_fraud_cluster_ = df_gmm_fraud_cluster_.drop(columns=['gmm_cluster_id'], axis=1)
    df_original = df_original.drop(columns=['gmm_cluster_id'], axis=1)

    # --------------------------------------------------------
    # Renaming columns
    # --------------------------------------------------------
    adrs_old = 'avg_wc_address_day_' + str(day)
    desc_old = 'avg_wc_description_day_' + str(day)
    note_old = 'avg_wc_notes_day_' + str(day)
    term_old = 'avg_wc_terms_day_' + str(day)
    clnt_old = 'client_count_day_' + str(day)
    invo_old = 'invoice_count_day_' + str(day)

    df_gmm_fraud_cluster_ = df_gmm_fraud_cluster_.rename(
        columns={adrs_old: "avg_wc_address",
                 desc_old: "avg_wc_description",
                 note_old: "avg_wc_notes",
                 term_old: "avg_wc_terms",
                 clnt_old: "client_count",
                 invo_old: "invoice_count"})

    # df_gmm_fraud_cluster_.to_csv('/Users/dewanferdouswahid/PycharmProjects/fraud_deploy/test/day_14_frc_inside_gmm.csv')
    return df_gmm_fraud_cluster_


# -----------------------------------------------
# Fitting Neural Network Classifier
# ------------------------------------------------
def nn_classifier(df_original, df_scaled, day):
    # GMM
    path0 = '/Users/dewanferdouswahid/PycharmProjects/fraud_deploy/saved_models/'
    path_nn = ''
    frd_cluster_id = -1

    if day == 7:
        path_nn = path0 + 'fdm_nn_day_07_k7_model.sav'
        frd_cluster_id = 1
    elif day == 14:
        path_nn = path0 + 'fdm_nn_day_14_k8_model.sav'
        frd_cluster_id = 1
    elif day == 21:
        path_nn = path0 + 'fdm_nn_day_21_k6_model.sav'
        frd_cluster_id = 5
    elif day == 28:
        path_nn = path0 + 'fdm_nn_day_28_k6_model.sav'
        frd_cluster_id = 5
    elif day == 35:
        path_nn = path0 + 'fdm_nn_day_35_k8_model.sav'
        frd_cluster_id = 5
    elif day == 42:
        path_nn = path0 + 'fdm_nn_day_42_k7_model.sav'
        frd_cluster_id = 1
    elif day == 49:
        path_nn = path0 + 'fdm_nn_day_49_k6_model.sav'
        frd_cluster_id = 2
    elif day == 56:
        path_nn = path0 + 'fdm_nn_day_56_k8_model.sav'
        frd_cluster_id = 5
    elif day == 63:
        path_nn = path0 + 'fdm_nn_day_63_k6_model.sav'
        frd_cluster_id = 5
    elif day == 70:
        path_nn = path0 + 'fdm_nn_day_70_k8_model.sav'
        frd_cluster_id = 7
    elif day == 77:
        path_nn = path0 + 'fdm_nn_day_77_k6_model.sav'
        frd_cluster_id = 1
    elif day == 84:
        path_nn = path0 + 'fdm_nn_day_84_k8_model.sav'
        frd_cluster_id = 1
    elif day == 91:
        path_nn = path0 + 'fdm_nn_day_91_k7_model.sav'
        frd_cluster_id = 5
    else:
        print ("Enter correct model day!")

    # -------------------------------------------------------
    # Load and fit NN model
    # -------------------------------------------------------

    # Load the saved gmm model
    nn_model_file = open(path_nn, 'rb')
    nn_model = pickle.load(nn_model_file)
    nn_model_file.close()

    # Predicting clustering
    nn_cluster_id = nn_model.predict(df_scaled)

    # Adding clusters id of each account to the dataframe
    df_original['nn_cluster_id'] = nn_cluster_id

    # -------------------------------------------------------
    # Filtering users accounts associated with fraud clusters
    # -------------------------------------------------------
    print("FRC ID: ", frd_cluster_id)

    # The only the accounts labeled as cluster c01 (fraud cluster)
    df_nn_fraud_cluster = df_original[df_original.nn_cluster_id == frd_cluster_id]

    df_nn_fraud_cluster_ = df_nn_fraud_cluster.reset_index()  # resetting index
    df_nn_fraud_cluster_ = df_nn_fraud_cluster_.drop(columns=['index'], axis=1)  # drop added columns during reindex

    # df_nn_fraud_cluster_.to_csv(
    #     '/Users/dewanferdouswahid/PycharmProjects/fraud_deploy/test/day_14_frc_inside_ndrp_nn.csv')

    # Dropping the cluster and class id label columns
    df_nn_fraud_cluster_ = df_nn_fraud_cluster_.drop(columns=['gmm_cluster_id'], axis=1)
    df_nn_fraud_cluster_ = df_nn_fraud_cluster_.drop(columns=['nn_cluster_id'], axis=1)

    df_original = df_original.drop(columns=['gmm_cluster_id'], axis=1)
    df_original = df_original.drop(columns=['nn_cluster_id'], axis=1)

    # --------------------------------------------------------
    # Renaming columns
    # --------------------------------------------------------
    adrs_old = 'avg_wc_address_day_' + str(day)
    desc_old = 'avg_wc_description_day_' + str(day)
    note_old = 'avg_wc_notes_day_' + str(day)
    term_old = 'avg_wc_terms_day_' + str(day)
    clnt_old = 'client_count_day_' + str(day)
    invo_old = 'invoice_count_day_' + str(day)

    df_nn_fraud_cluster_ = df_nn_fraud_cluster_.rename(
        columns={adrs_old: "avg_wc_address",
                 desc_old: "avg_wc_description",
                 note_old: "avg_wc_notes",
                 term_old: "avg_wc_terms",
                 clnt_old: "client_count",
                 invo_old: "invoice_count"})

    # df_nn_fraud_cluster_.to_csv('/Users/dewanferdouswahid/PycharmProjects/fraud_deploy/test/day_14_frc_inside_nn.csv')

    return df_nn_fraud_cluster_

