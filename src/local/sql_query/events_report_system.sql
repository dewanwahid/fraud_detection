SELECT *
  FROM (
   SELECT
       systemid,
       signup_date,
       date,
       event,
       SUM(count) AS count
   FROM (
       SELECT
           systemid,
           signup_date,
           event_key,
           lower(event) AS event
       FROM
           report_systems
           CROSS JOIN (
               SELECT
                   event,
                   event_key
               FROM
                   d_event
          )
       WHERE systemid ='4342936'
--            (smux_signup OR (account_origin IN ('helios', 'andromeda')))
--            AND signup_date BETWEEN '2018-08-01' and '2019-07-30'
   )
       LEFT JOIN event_counts USING (systemid, event_key)
       LEFT JOIN d_date USING (date_key)
   GROUP BY
       systemid,
       signup_date,
       date,
       event
HAVING date BETWEEN signup_date AND signup_date + INTERVAL '6 DAY'
      )
LEFT JOIN (select
                  systemid,
                  is_freshbooks_account_active,
                  is_new_trial_from_accountant_invite,
                  freshbooks_account_status,
                  is_paying,
                  base_subscription_amount_first_upgrade,
                  subscription_package_name,
                  upgrade_ever,
                  smux_signup,
                  account_origin,
                  signup_date,
                  signup_ip_address
      from report_systems
       WHERE
           (smux_signup OR (account_origin IN ('helios', 'andromeda')))
           AND signup_date BETWEEN '2019-01-01' and '2019-07-30') USING (systemid);



select systemid, signup_datetime from report_systems limit 10;

select systemid, id, event, created_at from events.events_raw_firehose limit 200;


select
    ec.systemid
,   e.event
,   date.date
,   ec.count
from event_counts ec
join d_date as date using (date_key)
join d_event as e using (event_key)
where systemid ='4342936'
order by count desc;


select *
from d_date;

----

WITH selected_accounts_events AS (
    SELECT systemid,
           signup_date,
           signup_datetime
    FROM report_systems
    WHERE signup_date BETWEEN '2018-08-01' and '2019-07-30'
), events_activities AS (
    SELECT sae.systemid,
           signup_date,
           ed.date,
           lower(e.event) as event,
           ec.count
    FROM selected_accounts_events AS sae
    LEFT JOIN event_counts AS ec USING (systemid)
    LEFT JOIN d_date AS ed USING (date_key)
    LEFT JOIN d_event e on ec.event_key = e.event_key
)
SELECT *
FROM events_activities;

----

WITH selected_accounts_events AS (
    SELECT systemid,
           signup_date,
           signup_datetime
    FROM report_systems
--     WHERE systemid ='4342936'
    WHERE signup_date BETWEEN '2019-06-01' and '2019-07-30'
), events_activities AS (
    SELECT sae.systemid,
           signup_date,
           dd.date,
           datediff(days, signup_date, dd.date) as days_to_event,
           lower(e.event) as event,
           ec.count
    FROM selected_accounts_events AS sae
    LEFT JOIN event_counts AS ec USING (systemid)
    LEFT JOIN d_date AS dd USING (date_key)
    LEFT JOIN d_event e on ec.event_key = e.event_key
), event_groupings AS (
    SELECT distinct  ea.systemid,
                    ea.signup_date,
                    ea.date,
                    ea.event,
                    ea.count,
                    (CASE WHEN days_to_event BETWEEN 0 AND 7 THEN ea.count END) AS day_7_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 14 THEN ea.count END) AS day_14_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 21 THEN ea.count END) AS day_21_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 28 THEN ea.count END) AS day_28_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 35 THEN ea.count END) AS day_35_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 42 THEN ea.count END) AS day_42_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 49 THEN ea.count END) AS day_49_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 56 THEN ea.count END) AS day_56_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 63 THEN ea.count END) AS day_63_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 70 THEN ea.count END) AS day_70_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 77 THEN ea.count END) AS day_77_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 84 THEN ea.count END) AS day_84_event,
                    (CASE WHEN days_to_event BETWEEN 0 AND 91 THEN ea.count END) AS day_91_event
    FROM events_activities AS ea
)
SELECT systemid,
       signup_date,
       date,
       event,
       count,
       sum(day_7_event) AS event_count_day_7,
       sum(day_14_event) AS event_count_day_14,
       sum(day_21_event) AS event_count_day_21,
       sum(day_28_event) AS event_count_day_28,
       sum(day_35_event) AS event_count_day_35,
       sum(day_42_event) AS event_count_day_42,
       sum(day_49_event) AS event_count_day_49,
       sum(day_56_event) AS event_count_day_56,
       sum(day_63_event) AS event_count_day_63,
       sum(day_70_event) AS event_count_day_70,
       sum(day_77_event) AS event_count_day_77,
       sum(day_84_event) AS event_count_day_84,
       sum(day_91_event) AS event_count_day_91
From event_groupings
GROUP BY systemid, signup_date, date, event, count
ORDER BY systemid, count DESC;
-- limit 500;










select *
from report_system_upgrades
where systemid = '4431498'

SELECT
    *
FROM
    report_subscribers
WHERE
    systemid = '4431498'

select *
from d_plan_change limit 10;

select *
from report_subscribers
limit 100;

select *
from repo