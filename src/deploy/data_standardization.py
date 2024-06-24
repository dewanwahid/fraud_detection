# -----------------------------------------------
# Standard library imports
# -----------------------------------------------
import pickle
import pandas


# -----------------------------------------------
# Data Standardization`
# -----------------------------------------------
def min_max_stnd(df_data_ana, day):
    print('Day : ', day)

    # Choose the appropriate saved model path
    path0 = '/Users/dewanferdouswahid/PycharmProjects/fraud_deploy/saved_models/'
    path_ = ''

    if day == 7:
        path_ = path0 + 'minmax_scaler_gmm_day_7.sav'
    elif day == 14:
        path_ = path0 + 'minmax_scaler_gmm_day_14.sav'
    elif day == 21:
        path_ = path0 + 'minmax_scaler_gmm_day_21.sav'
    elif day == 28:
        path_ = path0 + 'minmax_scaler_gmm_day_28.sav'
    elif day == 35:
        path_ = path0 + 'minmax_scaler_gmm_day_35.sav'
    elif day == 42:
        path_ = path0 + 'minmax_scaler_gmm_day_42.sav'
    elif day == 49:
        path_ = path0 + 'minmax_scaler_gmm_day_49.sav'
    elif day == 56:
        path_ = path0 + 'minmax_scaler_gmm_day_56.sav'
    elif day == 63:
        path_ = path0 + 'minmax_scaler_gmm_day_63.sav'
    elif day == 70:
        path_ = path0 + 'minmax_scaler_gmm_day_70.sav'
    elif day == 77:
        path_ = path0 + 'minmax_scaler_gmm_day_77.sav'
    elif day == 84:
        path_ = path0 + 'minmax_scaler_gmm_day_84.sav'
    elif day == 91:
        path_ = path0 + 'minmax_scaler_gmm_day_91.sav'
    else:
        print ("Enter correct model day!")

    # Load the saved model
    minmax_file = open(path_, 'rb')
    min_max_scaler = pickle.load(minmax_file)
    minmax_file.close()

    # List of columns to normalize
    column_names_to_not_normalize = ['admin_email', 'days_on_platform', 'effective_date',
                                     'is_sales_managed', 'signup_date', 'systemid']
    column_names_to_normalize = [x for x in list(df_data_ana) if x not in column_names_to_not_normalize]

    # List of features columns to be normalized (all except the 'systemid')
    x_ = df_data_ana[column_names_to_normalize].values

    # Fit data to the saved min-max model for normalizing
    x_scaled_ = min_max_scaler.fit_transform(x_)
    df_scaled = pandas.DataFrame(x_scaled_, columns=column_names_to_normalize, index=df_data_ana.index)
    # df_data_ana[column_names_to_normalize] = df_scaled

    df_stnd = df_scaled
    return df_stnd
