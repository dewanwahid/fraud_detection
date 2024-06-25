# ---------------------------------------
# Standard library imports
# ---------------------------------------
import pandas as pd
import re
from difflib import SequenceMatcher


# ------------------------------------------------------------------
# Word count function
# ------------------------------------------------------------------
def words_count(strg):
    if strg == '' or pd.isnull(strg):
        no_of_words = 0
    else:
        strg_words_list = re.findall(r"[\w']+", strg)
        no_of_words = len(strg_words_list)

    return no_of_words


# ------------------------------------------------------------------
# Function: Filtering FB test account by using admin email
# ------------------------------------------------------------------
def email_match(em, email_list):
    n = len(email_list)
    match_score = 0

    for ix in range(0, n):
        ch_email = str(email_list.loc[ix]['email'])

        if pd.isnull(em):
            match_score = 0
            break
        else:
            this_match_sc = SequenceMatcher(None, em, ch_email).ratio()
            match_score = max(match_score, this_match_sc)

    return match_score


# ------------------------------------------------------------------
# Function for aggregating selected column values
# ------------------------------------------------------------------

def cell_value_sum(row, cols):
    # print(row)
    sum_ = 0
    for i in cols:
        # print(i)
        # print(i, row[i])
        sum_ = sum_ + row[i]

    # print('Final sum: ', sum)
    return sum_


# ------------------------------------------------------------------
# Computing Fraud Risk Score (FRS)
# Computing Fraud Risk Score (FRS) based on the following formula:

# - 'days_on_platform' = $d$
# - 'declinedonlinepaymentnotification' = $p$
# - 'emailinvoice' = $e$
# - 'invoice_count_day_7' = $i$


# $frs\_gmm = \frac{p + e + i}{d}$
# ------------------------------------------------------------------


def fraud_risk_score(row):
    # selected column values
    d = row['model_name']
    p = float(row['declinedonlinepaymentnotification'])
    e = float(row['emailinvoice'])
    i = float(row['invoice_count'])

    # fraud risk score
    if d == 'D07':
        # print("D7")
        frs = (p + e + i) / 7
    elif d == 'D14':
        # print("D14")
        frs = (p + e + i) / 14
    elif d == 'D21':
        # print("D21")
        frs = (p + e + i) / 21
    elif d == 'D28':
        # print("D28")
        frs = (p + e + i) / 28
    elif d == 'D35':
        # print("D35")
        frs = (p + e + i) / 35
    elif d == 'D42':
        # print("D42")
        frs = (p + e + i) / 42
    elif d == 'D49':
        # print("D49")
        frs = (p + e + i) / 49
    else:
        # print("Exception")
        frs = 0
    return frs
