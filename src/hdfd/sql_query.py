def sql_for_test_data():
    sql_test = """SELECT * FROM `dataops-data-production.datamart_billing.d_billing_account` LIMIT 100;"""
    return sql_test


# ************************************************************************
# BQ SQL for Day 7 Model
# ************************************************************************
def sql_for_day_7_invoice_data():
    sql_day_7_invo = '''invoice created in last 7 days'''
    return sql_day_7_invo


def sql_for_day_7_rs_data():
    sql_day_7_rs = '''invoice details created in last 7 days'''
    return sql_day_7_rs


def sql_for_day_7_event():
    sql_day_7_event = '''all events related to invoices created last x days'''

    return sql_day_7_event


# ************************************************************************
# BQ SQL for Day 14 Model
# ************************************************************************


def sql_for_day_14_invoice_data():
    sql_day_14_invo = '''invoice created in last x days'''
    return sql_day_14_invo


def sql_for_day_14_rs_data():
    sql_day_14_rs = ''' invoice details'''
    return sql_day_14_rs


def sql_for_day_14_event():
    sql_day_14_event = ''' events details '''

    return sql_day_14_event


# ************************************************************************
# BQ SQL for Day 21 Model
# ************************************************************************

def sql_for_day_21_invoice_data():
    sql_day_21_invo = '''invoice in last x days'''
    return sql_day_21_invo


def sql_for_day_21_rs_data():
    sql_day_21_rs = '''invoice details'''
    return sql_day_21_rs


def sql_for_day_21_event():
    sql_day_21_event = '''event details'''

    return sql_day_21_event


# ************************************************************************
# BQ SQL for Day 28 Model
# ************************************************************************

def sql_for_day_28_invoice_data():
    sql_day_28_invo = '''invoice in last x days'''
    return sql_day_28_invo


def sql_for_day_28_rs_data():
    sql_day_28_rs = '''invoice details'''
    return sql_day_28_rs


def sql_for_day_28_event():
    sql_day_28_event = '''event details'''

    return sql_day_28_event


# ************************************************************************
# BQ SQL for Day 35 Model
# ************************************************************************

def sql_for_day_35_invoice_data():
    sql_day_35_invo = '''invoice in last x days'''
    return sql_day_35_invo


def sql_for_day_35_rs_data():
    sql_day_35_rs = '''invoice details'''
    return sql_day_35_rs


def sql_for_day_35_event():
    sql_day_35_event = '''event details'''

    return sql_day_35_event


# ************************************************************************
# BQ SQL for Day 42 Model
# ************************************************************************

def sql_for_day_42_invoice_data():
    sql_day_42_invo = '''invoice in last 42 days'''
    return sql_day_42_invo


def sql_for_day_42_rs_data():
    sql_day_42_rs = '''invoice details'''
    return sql_day_42_rs


def sql_for_day_42_event():
    sql_day_42_event = '''event details'''

    return sql_day_42_event


# ************************************************************************
# BQ SQL for Day 49 Model
# ************************************************************************

def sql_for_day_49_invoice_data():
    sql_day_49_invo = '''invoice in last x days'''
    return sql_day_49_invo


def sql_for_day_49_rs_data():
    sql_day_49_rs = '''invoice details'''
    return sql_day_49_rs


def sql_for_day_49_event():
    sql_day_49_event = '''event details'''

    return sql_day_49_event


# ************************************************************************
# BQ SQL for Day 56 Model
# ************************************************************************

def sql_for_day_56_invoice_data():
    sql_day_56_invo = '''invoice in last x days'''
    return sql_day_56_invo


def sql_for_day_56_rs_data():
    sql_day_56_rs = '''invoice details'''
    return sql_day_56_rs


def sql_for_day_56_event():
    sql_day_56_event = '''event details'''

    return sql_day_56_event


# ************************************************************************
# BQ SQL for Day 63 Model
# ************************************************************************

def sql_for_day_63_invoice_data():
    sql_day_63_invo = '''invoice in last 63 days'''
    return sql_day_63_invo


def sql_for_day_63_rs_data():
    sql_day_63_rs = '''invoice details'''
    return sql_day_63_rs


def sql_for_day_63_event():
    sql_day_63_event = '''event details'''

    return sql_day_63_event


# ************************************************************************
# BQ SQL for Day 70 Model
# ************************************************************************

def sql_for_day_70_invoice_data():
    sql_day_70_invo = '''invoice in last x days'''
    return sql_day_70_invo


def sql_for_day_70_rs_data():
    sql_day_70_rs = '''invoice details'''
    return sql_day_70_rs


def sql_for_day_70_event():
    sql_day_70_event = '''event details'''

    return sql_day_70_event


# ************************************************************************
# BQ SQL for Day 77 Model
# ************************************************************************

def sql_for_day_77_invoice_data():
    sql_day_77_invo = '''invoice in last x days'''
    return sql_day_77_invo


def sql_for_day_77_rs_data():
    sql_day_77_rs = '''invoice details'''
    return sql_day_77_rs


def sql_for_day_77_event():
    sql_day_77_event = '''event details'''

    return sql_day_77_event


# ************************************************************************
# BQ SQL for Day 84 Model
# ************************************************************************

def sql_for_day_84_invoice_data():
    sql_day_84_invo = '''invoice in last x days'''
    return sql_day_84_invo


def sql_for_day_84_rs_data():
    sql_day_84_rs = '''invoice details'''
    return sql_day_84_rs


def sql_for_day_84_event():
    sql_day_84_event = '''event details'''

    return sql_day_84_event


# ************************************************************************
# BQ SQL for Day 91 Model
# ************************************************************************

def sql_for_day_91_invoice_data():
    sql_day_91_invo = '''invoice in last x days'''
    return sql_day_91_invo


def sql_for_day_91_rs_data():
    sql_day_91_rs = '''invoice details'''
    return sql_day_91_rs


def sql_for_day_91_event():
    sql_day_91_event = '''event details'''

    return sql_day_91_event


# -------------------------------------------------------------
# Already labeled fraud data
# ------------------------------------------------------------
def sql_for_labeled_fraud_data():
    sql_labeled_fraud = '''labeled fraud data'''
    return sql_labeled_fraud
