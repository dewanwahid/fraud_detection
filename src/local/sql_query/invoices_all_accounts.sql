WITH invoices_in_a_period AS (
    SELECT systemid, signup_date
    FROM report_systems rs
    WHERE signup_date BETWEEN '2018-08-01' and '2019-07-30'
), invoice_created_at AS (
    SELECT
           pic.systemid,
           inv.invoiceid,
           pic.signup_date,
           inv.create_date,
           inv.created_at,
           inv.description,
           inv.notes,
           inv.terms,
           inv.address,
           DATEDIFF(days, pic.signup_date, inv.created_at) AS days_to_invoice_creation
    FROM invoices_in_a_period AS pic
    LEFT JOIN coalesced_live_shards.invoice_stable as inv USING (systemid)
    WHERE ((days_to_invoice_creation BETWEEN 0 AND 7) OR days_to_invoice_creation IS NULL)
)

SELECT *
FROM invoice_created_at;
-- WHERE invoiceid IS NULL;



---

select *
from coalesced_live_shards.invoice_stable
limit 5;

select * from report_systems where systemid !~ '^[0-9]';