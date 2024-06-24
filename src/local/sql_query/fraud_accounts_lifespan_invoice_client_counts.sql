WITH fraud_systems AS ( -- THIS IS A FILTER
    SELECT systemid FROM data_science.fraud_accounts_list GROUP BY 1
), fraud_status_date AS (
    SELECT systemid,
           status_date AS last_date
    FROM data_science.fraud_accounts_list
    JOIN fraud_systems USING(systemid)
),  client_dates AS (
    SELECT
        u.systemid,
        u.userid,
        u.signup_date,
        DATEDIFF(days, rs.signup_date, u.signup_date) AS days_to_client_creation
    from coalesced_live_shards."user" AS u
    LEFT JOIN report_systems rs USING(systemid)
    WHERE u.level = 0
),  client_groupings AS (
    SELECT
        systemid,
        SUM(CASE WHEN days_to_client_creation BETWEEN 0 AND 8 THEN 1 ELSE 0 END) AS client_count_day_7,
        SUM(case when days_to_client_creation BETWEEN  0 AND 16 THEN 1 ELSE 0 END) AS client_count_day_15,
        SUM(case when days_to_client_creation BETWEEN  0 AND 31 THEN 1 ELSE 0 END) AS client_count_day_30,
        SUM(case when days_to_client_creation BETWEEN  0 AND 46 THEN 1 ELSE 0 END) AS client_count_day_45,
        SUM(case when days_to_client_creation BETWEEN  0 AND 61 THEN 1 ELSE 0 END) AS client_count_day_60,
        SUM(case when days_to_client_creation BETWEEN  0 AND 76 THEN 1 ELSE 0 END) AS client_count_day_75,
        SUM(case when days_to_client_creation BETWEEN 0 AND 91 THEN 1 ELSE 0 END) AS client_count_day_90,
        SUM(case when days_to_client_creation BETWEEN 0 AND 181 THEN 1 ELSE 0 END) AS client_count_month_6,
        SUM(case when days_to_client_creation BETWEEN 0 AND 366 THEN 1 ELSE 0 END) AS client_count_year_1,
        SUM(case when days_to_client_creation BETWEEN 0 AND 731 THEN 1 ELSE 0 END) AS client_count_year_2,
        SUM(case when days_to_client_creation BETWEEN 0 AND 1096 THEN 1 ELSE 0 END) AS client_count_year_3,
        SUM(case when days_to_client_creation BETWEEN 0 AND 1461 THEN 1 ELSE 0 END) AS client_count_year_4,
        SUM(case when days_to_client_creation BETWEEN 0 AND (5*365+1) THEN 1 ELSE 0 END) AS client_count_year_5,
        SUM(case when days_to_client_creation BETWEEN 0 AND (6*365+1) THEN 1 ELSE 0 END) AS client_count_year_6,
        SUM(case when days_to_client_creation BETWEEN 0 AND(7*365+1) THEN 1 ELSE 0 END) AS client_count_year_7,
        SUM(case when days_to_client_creation BETWEEN 0 AND (8*365+1) THEN 1 ELSE 0 END) AS client_count_year_8,
        SUM(case when days_to_client_creation BETWEEN 0 AND (9*365+1) THEN 1 ELSE 0 END) AS client_count_year_9,
        SUM(case when days_to_client_creation BETWEEN 0 AND (10*365+1) THEN 1 ELSE 0 END) AS client_count_year_10
    FROM client_dates
    GROUP BY 1
), invoice_dates AS (
    SELECT
           inv.systemid,
           inv.invoiceid,
           inv.create_date,
           DATEDIFF(DAYS, rs.signup_date, inv.create_date) AS days_to_invoice_creation
    FROM coalesced_live_shards.invoice AS inv
             LEFT JOIN report_systems rs USING (systemid)
    WHERE inv.active = 1
), invoice_groupings AS (
    SELECT
        systemid,
        COUNT(invoiceid) AS invoice_count,
        SUM(CASE WHEN days_to_invoice_creation BETWEEN 0 AND 8 THEN 1 ELSE 0 END) AS invoice_count_day_7,
        sum(case when days_to_invoice_creation between 0 and 16 THEN 1 ELSE 0 END) AS invoice_count_day_15,
        sum(case when days_to_invoice_creation between 0 and 31 THEN 1 ELSE 0 END) AS invoice_count_day_30,
        sum(case when days_to_invoice_creation between 0 and 46 THEN 1 ELSE 0 END) AS invoice_count_day_45,
        sum(case when days_to_invoice_creation between 0 and 61 THEN 1 ELSE 0 END) AS invoice_count_day_60,
        sum(case when days_to_invoice_creation between 0 and 76 THEN 1 ELSE 0 END) AS invoice_count_day_75,
        sum(case when days_to_invoice_creation between 0 and 91 THEN 1 ELSE 0 END) AS invoice_count_day_90,
        sum(case when days_to_invoice_creation between 0 and 181 THEN 1 ELSE 0 END) AS invoice_count_month_6,
        sum(case when days_to_invoice_creation between 0 and (1*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_1,
        sum(case when days_to_invoice_creation between 0 and (2*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_2,
        sum(case when days_to_invoice_creation between 0 and (3*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_3,
        sum(case when days_to_invoice_creation between 0 and (4*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_4,
        sum(case when days_to_invoice_creation between 0 and (5*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_5,
        sum(case when days_to_invoice_creation between 0 and (6*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_6,
        sum(case when days_to_invoice_creation between 0 and (7*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_7,
        sum(case when days_to_invoice_creation between 0 and (8*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_8,
        sum(case when days_to_invoice_creation between 0 and (9*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_9,
        SUM(case when days_to_invoice_creation between 0 and (10*365+1) THEN 1 ELSE 0 END) AS invoice_count_year_10
    FROM invoice_dates
    GROUP BY 1
)
SELECT
    a.systemid ,
    a.client_count,
    inv.invoice_count,
    signup_date,
    fr.last_date::DATE AS last_date,
    DATEDIFF(DAY, signup_date, fr.last_date::DATE) AS days_to_ban_hammer,
    c.client_count_day_7,
    c.client_count_day_15,
    c.client_count_day_30,
    c.client_count_day_45,
    c.client_count_day_60,
    c.client_count_day_75,
    c.client_count_day_90,
    c.client_count_month_6,
    c.client_count_year_1,
    c.client_count_year_2,
    c.client_count_year_3,
    c.client_count_year_4,
    c.client_count_year_5,
    c.client_count_year_6,
    c.client_count_year_7,
    c.client_count_year_8,
    c.client_count_year_9,
    c.client_count_year_10,
    inv.invoice_count_day_7,
    inv.invoice_count_day_15,
    inv.invoice_count_day_30,
    inv.invoice_count_day_45,
    inv.invoice_count_day_60,
    inv.invoice_count_day_75,
    inv.invoice_count_day_90,
    inv.invoice_count_month_6,
    inv.invoice_count_year_1,
    inv.invoice_count_year_2,
    inv.invoice_count_year_3,
    inv.invoice_count_year_4,
    inv.invoice_count_year_5,
    inv.invoice_count_year_6,
    inv.invoice_count_year_7,
    inv.invoice_count_year_8,
    inv.invoice_count_year_9,
    inv.invoice_count_year_10
FROM report_systems a

JOIN fraud_status_date AS fr USING(systemid)
LEFT JOIN client_groupings c USING(systemid)
LEFT JOIN invoice_groupings inv USING (systemid)
ORDER BY days_to_ban_hammer DESC;