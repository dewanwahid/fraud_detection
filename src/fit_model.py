# ---------------------------------------
# Standard library imports
# ---------------------------------------
import pickle


# -----------------------------------------------
# Fitting unsupervised GMM model
# ------------------------------------------------
def unsupervised_gmm(df_original, df_scaled, day):
    # GMM
    path0 = ''
    path_ = ''
    frd_cluster_id = -1

    if day == 7:
        path_ = path0 + 'gmm_saved_model_d7.sav'
        frd_cluster_id = 1
    elif day == 14:
        path_ = path0 + 'gmm_saved_model_d14.sav'
        frd_cluster_id = 1
    elif day == 21:
        path_ = path0 + 'gmm_saved_model_d21.sav'
        frd_cluster_id = 5
    elif day == 28:
        path_ = path0 + 'gmm_saved_model_d28.sav'
        frd_cluster_id = 5
    elif day == 35:
        path_ = path0 + 'gmm_saved_model_d35.sav'
        frd_cluster_id = 5
    elif day == 42:
        path_ = path0 + 'gmm_saved_model_d42.sav'
        frd_cluster_id = 1
    elif day == 49:
        path_ = path0 + 'gmm_saved_model_d49.sav'
        frd_cluster_id = 2
    elif day == 56:
        path_ = path0 + 'gmm_saved_model_d56.sav'
        frd_cluster_id = 5
    elif day == 63:
        path_ = path0 + 'gmm_saved_model_d63.sav'
        frd_cluster_id = 5
    elif day == 70:
        path_ = path0 + 'gmm_saved_model_d70.sav'
        frd_cluster_id = 7
    elif day == 77:
        path_ = path0 + 'gmm_saved_model_d77.sav'
        frd_cluster_id = 1
    elif day == 84:
        path_ = path0 + 'gmm_saved_model_d84.sav'
        frd_cluster_id = 1
    elif day == 91:
        path_ = path0 + 'gmm_saved_model_d91.sav'
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
    df1 = df_original[df_original.gmm_cluster_id == frd_cluster_id]

    df1_ = df1.reset_index()  # resetting index
    df1_ = df1_.drop(columns=['index'], axis=1)  # drop added columns during reindex


    # Dropping the cluster and class id label columns
    df1_ = df1_.drop(columns=['gmm_cluster_id'], axis=1)
    df_original = df_original.drop(columns=['gmm_cluster_id'], axis=1)

    # --------------------------------------------------------
    # Renaming columns
    # --------------------------------------------------------
    adrs_old = 'avg_wc_invo_str1_' + str(day)
    desc_old = 'avg_wc_invo_str2_' + str(day)
    note_old = 'avg_wc_invo_str1_' + str(day)
    term_old = 'avg_wc_invo_str1_' + str(day)
    clnt_old = 'clnt_ct_' + str(day)
    invo_old = 'invo_ct_' + str(day)

    df1_ = df1_.rename(
        columns={adrs_old: "avg_wc_invo_str1",
                 desc_old: "avg_wc_invo_str2",
                 note_old: "avg_wc_invo_str3",
                 term_old: "avg_wc_invo_str4",
                 clnt_old: "clnt_ct",
                 invo_old: "invo_ct"})

    return df1_


# -----------------------------------------------
# Fitting Neural Network Classifier
# ------------------------------------------------
def nn_classifier(df_original, df_scaled, day):
    # GMM
    path0 = ''
    path_nn = ''
    frd_cluster_id = -1

    if day == 7:
        path_nn = path0 + 'nn_d07_model.sav'
        frd_cluster_id = 1
    elif day == 14:
        path_nn = path0 + 'nn_d14_model.sav'
        frd_cluster_id = 1
    elif day == 21:
        path_nn = path0 + 'nn_d21_model.sav'
        frd_cluster_id = 5
    elif day == 28:
        path_nn = path0 + 'nn_d28_model.sav'
        frd_cluster_id = 5
    elif day == 35:
        path_nn = path0 + 'nn_d35_model.sav'
        frd_cluster_id = 5
    elif day == 42:
        path_nn = path0 + 'nn_d42_model.sav'
        frd_cluster_id = 1
    elif day == 49:
        path_nn = path0 + 'nn_d49_model.sav'
        frd_cluster_id = 2
    elif day == 56:
        path_nn = path0 + 'nn_d56_model.sav'
        frd_cluster_id = 5
    elif day == 63:
        path_nn = path0 + 'nn_d63_model.sav'
        frd_cluster_id = 5
    elif day == 70:
        path_nn = path0 + 'nn_d70_model.sav'
        frd_cluster_id = 7
    elif day == 77:
        path_nn = path0 + 'nn_d77_model.sav'
        frd_cluster_id = 1
    elif day == 84:
        path_nn = path0 + 'nn_d84_model.sav'
        frd_cluster_id = 1
    elif day == 91:
        path_nn = path0 + 'nn_d91_model.sav'
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
    df2 = df_original[df_original.nn_cluster_id == frd_cluster_id]

    df2_ = df2.reset_index()  # resetting index
    df2_ = df2_.drop(columns=['index'], axis=1)  # drop added columns during reindex


    # Dropping the cluster and class id label columns
    df2_ = df2_.drop(columns=['gmm_cluster_id'], axis=1)
    df2_ = df2_.drop(columns=['nn_cluster_id'], axis=1)

    df_original = df_original.drop(columns=['gmm_cluster_id'], axis=1)
    df_original = df_original.drop(columns=['nn_cluster_id'], axis=1)

    # --------------------------------------------------------
    # Renaming columns
    # --------------------------------------------------------
    adrs_old = 'avg_wc_invo_str1_' + str(day)
    desc_old = 'avg_wc_invo_str2_' + str(day)
    note_old = 'avg_wc_invo_str3_' + str(day)
    term_old = 'avg_wc_invo_str4_' + str(day)
    clnt_old = 'clnt_ct_' + str(day)
    invo_old = 'invo_ct_' + str(day)

    df2_ = df2_.rename(
        columns={adrs_old: "avg_wc_invo_str1_",
                 desc_old: "avg_wc_invo_str2_",
                 note_old: "avg_wc_invo_str3_",
                 term_old: "avg_wc_invo_str4_",
                 clnt_old: "clnt_ct_",
                 invo_old: "invo_ct_"})

    return df2_

