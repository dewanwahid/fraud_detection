def sql_for_test_data():
    sql_test = """SELECT * FROM `dataops-data-production.datamart_billing.d_billing_account` LIMIT 100;"""
    return sql_test


# ************************************************************************
# BQ SQL for Day 7 Model
# ************************************************************************
def sql_for_day_7_invoice_data():
    sql_day_7_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 13 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 7))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_7_invo


def sql_for_day_7_rs_data():
    sql_day_7_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 13 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS invoice_count_day_7
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS client_count_day_7

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_7,
               cl_gr.client_count_day_7
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_7_rs


def sql_for_day_7_event():
    sql_day_7_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 13 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 7 THEN ea.count END) AS day_7_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_7_event) AS event_count_day_7
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_7 DESC;
    '''

    return sql_day_7_event


# ************************************************************************
# BQ SQL for Day 14 Model
# ************************************************************************


def sql_for_day_14_invoice_data():
    sql_day_14_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 20 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 14))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_14_invo


def sql_for_day_14_rs_data():
    sql_day_14_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 20 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 14 THEN 1 ELSE 0 END) AS invoice_count_day_14
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 14 THEN 1 ELSE 0 END) AS client_count_day_14

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_14,
               cl_gr.client_count_day_14
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_14_rs


def sql_for_day_14_event():
    sql_day_14_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 20 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 14 THEN ea.count END) AS day_14_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_14_event) AS event_count_day_14
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_14 DESC;
    '''

    return sql_day_14_event


# ************************************************************************
# BQ SQL for Day 21 Model
# ************************************************************************

def sql_for_day_21_invoice_data():
    sql_day_21_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 21 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 21))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_21_invo


def sql_for_day_21_rs_data():
    sql_day_21_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 21 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 21 THEN 1 ELSE 0 END) AS invoice_count_day_21
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 21 THEN 1 ELSE 0 END) AS client_count_day_21

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_21,
               cl_gr.client_count_day_21
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_21_rs


def sql_for_day_21_event():
    sql_day_21_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 21 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 21 THEN ea.count END) AS day_21_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_21_event) AS event_count_day_21
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_21 DESC;
    '''

    return sql_day_21_event


# ************************************************************************
# BQ SQL for Day 28 Model
# ************************************************************************

def sql_for_day_28_invoice_data():
    sql_day_28_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 34 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 28))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_28_invo


def sql_for_day_28_rs_data():
    sql_day_28_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 34 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 28 THEN 1 ELSE 0 END) AS invoice_count_day_28
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 28 THEN 1 ELSE 0 END) AS client_count_day_28

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_28,
               cl_gr.client_count_day_28
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_28_rs


def sql_for_day_28_event():
    sql_day_28_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 34 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 28 THEN ea.count END) AS day_28_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_28_event) AS event_count_day_28
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_28 DESC;
    '''

    return sql_day_28_event


# ************************************************************************
# BQ SQL for Day 35 Model
# ************************************************************************

def sql_for_day_35_invoice_data():
    sql_day_35_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 41 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 35 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 35))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_35_invo


def sql_for_day_35_rs_data():
    sql_day_35_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 41 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 35 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 35 THEN 1 ELSE 0 END) AS invoice_count_day_35
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 35 THEN 1 ELSE 0 END) AS client_count_day_35

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_35,
               cl_gr.client_count_day_35
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_35_rs


def sql_for_day_35_event():
    sql_day_35_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 41 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 35 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 35 THEN ea.count END) AS day_35_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_35_event) AS event_count_day_35
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_35 DESC;
    '''

    return sql_day_35_event


# ************************************************************************
# BQ SQL for Day 42 Model
# ************************************************************************

def sql_for_day_42_invoice_data():
    sql_day_42_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 48 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 42 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 42))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_42_invo


def sql_for_day_42_rs_data():
    sql_day_42_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 48 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 42 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 42 THEN 1 ELSE 0 END) AS invoice_count_day_42
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 42 THEN 1 ELSE 0 END) AS client_count_day_42

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_42,
               cl_gr.client_count_day_42
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_42_rs


def sql_for_day_42_event():
    sql_day_42_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 48 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 42 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 42 THEN ea.count END) AS day_42_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_42_event) AS event_count_day_42
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_42 DESC;
    '''

    return sql_day_42_event


# ************************************************************************
# BQ SQL for Day 49 Model
# ************************************************************************

def sql_for_day_49_invoice_data():
    sql_day_49_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 55 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 49 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 49))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_49_invo


def sql_for_day_49_rs_data():
    sql_day_49_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 55 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 49 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 49 THEN 1 ELSE 0 END) AS invoice_count_day_49
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 49 THEN 1 ELSE 0 END) AS client_count_day_49

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_49,
               cl_gr.client_count_day_49
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_49_rs


def sql_for_day_49_event():
    sql_day_49_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 55 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 49 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 49 THEN ea.count END) AS day_49_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_49_event) AS event_count_day_49
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_49 DESC;
    '''

    return sql_day_49_event


# ************************************************************************
# BQ SQL for Day 56 Model
# ************************************************************************

def sql_for_day_56_invoice_data():
    sql_day_56_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 62 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 56 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 56))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_56_invo


def sql_for_day_56_rs_data():
    sql_day_56_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 62 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 56 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 56 THEN 1 ELSE 0 END) AS invoice_count_day_56
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 56 THEN 1 ELSE 0 END) AS client_count_day_56

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_56,
               cl_gr.client_count_day_56
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_56_rs


def sql_for_day_56_event():
    sql_day_56_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 62 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 56 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 56 THEN ea.count END) AS day_56_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_56_event) AS event_count_day_56
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_56 DESC;
    '''

    return sql_day_56_event


# ************************************************************************
# BQ SQL for Day 63 Model
# ************************************************************************

def sql_for_day_63_invoice_data():
    sql_day_63_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 69 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 63 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 63))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_63_invo


def sql_for_day_63_rs_data():
    sql_day_63_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 69 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 63 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 63 THEN 1 ELSE 0 END) AS invoice_count_day_63
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 63 THEN 1 ELSE 0 END) AS client_count_day_63

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_63,
               cl_gr.client_count_day_63
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_63_rs


def sql_for_day_63_event():
    sql_day_63_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 69 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 63 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 63 THEN ea.count END) AS day_63_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_63_event) AS event_count_day_63
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_63 DESC;
    '''

    return sql_day_63_event


# ************************************************************************
# BQ SQL for Day 70 Model
# ************************************************************************

def sql_for_day_70_invoice_data():
    sql_day_70_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 76 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 70 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 70))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_70_invo


def sql_for_day_70_rs_data():
    sql_day_70_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 76 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 70 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 70 THEN 1 ELSE 0 END) AS invoice_count_day_70
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 70 THEN 1 ELSE 0 END) AS client_count_day_70

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_70,
               cl_gr.client_count_day_70
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_70_rs


def sql_for_day_70_event():
    sql_day_70_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 76 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 70 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 70 THEN ea.count END) AS day_70_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_70_event) AS event_count_day_70
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_70 DESC;
    '''

    return sql_day_70_event


# ************************************************************************
# BQ SQL for Day 77 Model
# ************************************************************************

def sql_for_day_77_invoice_data():
    sql_day_77_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 83 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 77 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 77))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_77_invo


def sql_for_day_77_rs_data():
    sql_day_77_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 83 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 77 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 77 THEN 1 ELSE 0 END) AS invoice_count_day_77
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 77 THEN 1 ELSE 0 END) AS client_count_day_77

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_77,
               cl_gr.client_count_day_77
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_77_rs


def sql_for_day_77_event():
    sql_day_77_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 83 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 77 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 77 THEN ea.count END) AS day_77_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_77_event) AS event_count_day_77
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_77 DESC;
    '''

    return sql_day_77_event


# ************************************************************************
# BQ SQL for Day 84 Model
# ************************************************************************

def sql_for_day_84_invoice_data():
    sql_day_84_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 84 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 84))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_84_invo


def sql_for_day_84_rs_data():
    sql_day_84_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 84 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 84 THEN 1 ELSE 0 END) AS invoice_count_day_84
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 84 THEN 1 ELSE 0 END) AS client_count_day_84

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_84,
               cl_gr.client_count_day_84
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_84_rs


def sql_for_day_84_event():
    sql_day_84_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 84 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 84 THEN ea.count END) AS day_84_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_84_event) AS event_count_day_84
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_84 DESC;
    '''

    return sql_day_84_event


# ************************************************************************
# BQ SQL for Day 91 Model
# ************************************************************************

def sql_for_day_91_invoice_data():
    sql_day_91_invo = '''WITH invoices_in_a_period AS (
                    SELECT
                            systemid,
                            signup_date
                    FROM dataops-data-production.datamart_reports.report_systems rs
                    WHERE signup_date between DATE_SUB(CURRENT_DATE, INTERVAL 97 DAY) and DATE_SUB(CURRENT_DATE, INTERVAL 91 DAY)
                ), invoice_created_at AS (
                    SELECT
                           pic.systemid,
                           pic.signup_date,
                           inv.invoiceid,
                           inv.create_date,
                           inv.description,
                           inv.notes,
                           inv.terms,
                           inv.address,
                           DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
                    FROM invoices_in_a_period AS pic
                    LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice as inv USING (systemid)
                    WHERE ((DATE_DIFF(inv.created_at, pic.signup_date, DAY) BETWEEN 0 AND 91))
                )
    
                SELECT *
                FROM invoice_created_at;'''
    return sql_day_91_invo


def sql_for_day_91_rs_data():
    sql_day_91_rs = '''WITH periodic_report_system_activities AS (
            SELECT
                systemid,
                signup_date,
                admin_email,
                is_sales_managed,
                is_freshbooks_account_active,
                is_paying
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 97 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 91 DAY) AND is_sales_managed = 0
        ), invoice_create_date AS (
            SELECT
                   pic.systemid,
                   inv.invoiceid,
                   inv.create_date,
                   inv.created_at,
                   DATE_DIFF(inv.created_at, pic.signup_date, DAY) AS days_to_invoice_creation
            FROM periodic_report_system_activities AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.invoice AS inv USING (systemid)
        ), invoice_grouping AS (
            SELECT
                   systemid,
                   COUNT(invoiceid) as invoice_count,
                   SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 91 THEN 1 ELSE 0 END) AS invoice_count_day_91
            FROM invoice_create_date
            GROUP BY systemid
        ), client_crate_date AS (
             SELECT
                    pic.systemid,
                    usr.userid,
                    usr.signup_date,
                    DATE_DIFF(usr.signup_date, pic.signup_date, DAY) AS days_to_client_creation
            FROM periodic_report_system_activities  AS pic
            LEFT JOIN dataops-replica-production.coalesced_live_shards.user as usr USING (systemid)
        ), client_grouping AS (
            SELECT
                   systemid,
                   count(userid) AS client_count,
                   SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 91 THEN 1 ELSE 0 END) AS client_count_day_91

            FROM  client_crate_date
            GROUP BY systemid
        )

        SELECT
               systemid,
               signup_date,
               current_date as effective_date,
               DATE_DIFF(current_date, signup_date, DAY) as days_on_platform,
               admin_email,
               is_sales_managed,
               is_freshbooks_account_active,
               is_paying,
               inv_gr.invoice_count_day_91,
               cl_gr.client_count_day_91
        FROM periodic_report_system_activities
        LEFT JOIN invoice_grouping as inv_gr USING (systemid)
        LEFT JOIN client_grouping AS cl_gr USING (systemid);
        '''
    return sql_day_91_rs


def sql_for_day_91_event():
    sql_day_91_event = '''WITH selected_accounts_events AS (
            SELECT systemid,
                   signup_date,
                   signup_datetime
            FROM dataops-data-production.datamart_reports.report_systems rs
            WHERE signup_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 97 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 91 DAY) AND is_sales_managed = 0
        ), events_activities AS (
            SELECT sae.systemid,
                   signup_date,
                   dd.date,
                   DATE_DIFF(signup_date, dd.date, DAY) as days_to_event,
                   lower(e.event) as event,
                   ec.count
            FROM selected_accounts_events AS sae
            LEFT JOIN `dataops-data-production.datamart_events.event_counts` AS ec USING (systemid)
            LEFT JOIN `dataops-data-production.datamart_common.d_date` AS dd USING (date_key)
            LEFT JOIN `dataops-data-production.datamart_events.d_event` e on ec.event_key = e.event_key
        ), event_groupings AS (
            SELECT DISTINCT ea.systemid,
                            ea.signup_date,
                            ea.date,
                            ea.event,
                            ea.count,
                            (CASE WHEN days_to_event BETWEEN 0 AND 91 THEN ea.count END) AS day_91_event
            FROM events_activities AS ea
        )
        SELECT systemid,
               event,
               sum(day_91_event) AS event_count_day_91
        From event_groupings
        GROUP BY systemid, signup_date, event
        ORDER BY systemid, event_count_day_91 DESC;
    '''

    return sql_day_91_event


# -------------------------------------------------------------
# Already labeled fraud data
# ------------------------------------------------------------
def sql_for_labeled_fraud_data():
    sql_labeled_fraud = '''SELECT systemid FROM `fb-data-science-dev.fraud_model_output.fraud_non_fraud_labeled`'''
    return sql_labeled_fraud
