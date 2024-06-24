WITH periodic_report_system_activities AS (
    SELECT systemid, signup_date, is_freshbooks_account_active, is_new_trial_from_accountant_invite,
           freshbooks_account_status, is_paying, base_subscription_amount_first_upgrade,
           subscription_package_name, upgrade_ever, signup_ip_address
    FROM report_systems rs
    WHERE signup_date BETWEEN '2018-08-01' and '2019-07-30'
), invoice_create_date AS (
    SELECT
           pic.systemid,
           inv.invoiceid,
           inv.create_date,
           inv.created_at,
           DATEDIFF(days, pic.signup_date, inv.created_at) AS days_to_invoice_creation
    FROM periodic_report_system_activities AS pic
    LEFT JOIN coalesced_live_shards.invoice_stable as inv USING (systemid)
), invoice_grouping AS (
    SELECT
           systemid,
           COUNT(invoiceid) as invoice_count,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS invoice_count_day_7,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 14 THEN 1 ELSE 0 END) AS invoice_count_day_14,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 21 THEN 1 ELSE 0 END) AS invoice_count_day_21,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 28 THEN 1 ELSE 0 END) AS invoice_count_day_28,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 35 THEN 1 ELSE 0 END) AS invoice_count_day_35,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 42 THEN 1 ELSE 0 END) AS invoice_count_day_42,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 49 THEN 1 ELSE 0 END) AS invoice_count_day_49,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 56 THEN 1 ELSE 0 END) AS invoice_count_day_56,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 63 THEN 1 ELSE 0 END) AS invoice_count_day_63,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 70 THEN 1 ELSE 0 END) AS invoice_count_day_70,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 77 THEN 1 ELSE 0 END) AS invoice_count_day_77,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 84 THEN 1 ELSE 0 END) AS invoice_count_day_84,
           SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 91 THEN 1 ELSE 0 END) AS invoice_count_day_91
    FROM invoice_create_date
    GROUP BY systemid
), client_crate_date AS (
     SELECT
            pic.systemid,
            usr.userid,
            usr.signup_date,
            DATEDIFF(days, pic.signup_date, usr.signup_date) AS days_to_client_creation
    FROM periodic_report_system_activities  AS pic
    LEFT JOIN coalesced_live_shards."user" as usr USING (systemid)
), client_grouping AS (
    SELECT
           systemid,
           count(userid) AS client_count,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS client_count_day_7,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 14 THEN 1 ELSE 0 END) AS client_count_day_14,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 21 THEN 1 ELSE 0 END) AS client_count_day_21,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 28 THEN 1 ELSE 0 END) AS client_count_day_28,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 35 THEN 1 ELSE 0 END) AS client_count_day_35,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 42 THEN 1 ELSE 0 END) AS client_count_day_42,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 49 THEN 1 ELSE 0 END) AS client_count_day_49,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 56 THEN 1 ELSE 0 END) AS client_count_day_56,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 63 THEN 1 ELSE 0 END) AS client_count_day_63,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 70 THEN 1 ELSE 0 END) AS client_count_day_70,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 77 THEN 1 ELSE 0 END) AS client_count_day_77,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 84 THEN 1 ELSE 0 END) AS client_count_day_84,
           SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 91 THEN 1 ELSE 0 END) AS client_count_day_91
    FROM  client_crate_date
    GROUP BY systemid
)

SELECT
       systemid, signup_date,
       is_freshbooks_account_active, is_new_trial_from_accountant_invite,
       freshbooks_account_status, is_paying, base_subscription_amount_first_upgrade,
       subscription_package_name, upgrade_ever, signup_ip_address,
       inv_gr.invoice_count,
       inv_gr.invoice_count_day_7,
       inv_gr.invoice_count_day_14,
       inv_gr.invoice_count_day_21,
       inv_gr.invoice_count_day_28,
       inv_gr.invoice_count_day_35,
       inv_gr.invoice_count_day_42,
       inv_gr.invoice_count_day_49,
       inv_gr.invoice_count_day_56,
       inv_gr.invoice_count_day_63,
       inv_gr.invoice_count_day_70,
       inv_gr.invoice_count_day_77,
       inv_gr.invoice_count_day_84,
       inv_gr.invoice_count_day_91,
       cl_gr.client_count,
       cl_gr.client_count_day_7,
       cl_gr.client_count_day_14,
       cl_gr.client_count_day_21,
       cl_gr.client_count_day_28,
       cl_gr.client_count_day_35,
       cl_gr.client_count_day_42,
       cl_gr.client_count_day_49,
       cl_gr.client_count_day_56,
       cl_gr.client_count_day_63,
       cl_gr.client_count_day_70,
       cl_gr.client_count_day_77,
       cl_gr.client_count_day_84,
       cl_gr.client_count_day_91
FROM periodic_report_system_activities
LEFT JOIN invoice_grouping as inv_gr USING (systemid)
LEFT JOIN client_grouping AS cl_gr USING (systemid);