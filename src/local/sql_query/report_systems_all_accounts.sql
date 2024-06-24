SELECT
       systemid,
       business_id,
       admin_identity_id,
       is_freshbooks_account_active,
       is_modern,
       country_code,
       country,
       postal_code,
       street,
       mob_phone,
       bus_phone,
       admin_fname,
       admin_lname,
       business_name,
       admin_email,
       signup_datetime,
       signup_date,
       signup_ip_address,
       smux_signup,
       merged_channel_category_1,
       client_count
FROM report_systems
WHERE signup_date BETWEEN '2018-08-01' and '2019-07-30';