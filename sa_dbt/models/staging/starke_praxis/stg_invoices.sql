SELECT
        raw_invoices.Id AS invoice_id,
        raw_invoices.Nummer AS invoice_number,
        NULLIF(raw_invoices.StornoNummer, '') AS cancellation_invoice_number,
        raw_invoices.Nachberechnung::BOOLEAN AS subsequent_billing,
        raw_invoices.REZ_Nr AS receipt_id,
        raw_invoices.SEN_Nr AS delivery_id,
        DATE '1801-01-01' + to_days(raw_invoices.Datum::INT) AS invoice_date,
        DATE '1801-01-01' + to_days(raw_invoices.Von::INT) AS invoice_valid_from,
        DATE '1801-01-01' + to_days(raw_invoices.Bis::INT) AS invoice_valid_to,
        DATE '1801-01-01' + to_days(raw_invoices.Faelligkeitsdatum::INT) AS due_date,

        -- Dunning dates
        -- In the raw data there is a second dunning date but no first dunning date, a third dunning date but no second dunning date, etc.
        -- The following code is a workaround to get the correct dunning dates
        DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_invoices.Mahndatum_1, 0), NULLIF(raw_invoices.Mahndatum_2, 0), NULLIF(raw_invoices.Mahndatum_3, 0))::INT) AS first_dunning_date,
        NULLIF(DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_invoices.Mahndatum_2, 0), NULLIF(raw_invoices.Mahndatum_3, 0))::INT), DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_invoices.Mahndatum_1, 0), NULLIF(raw_invoices.Mahndatum_2, 0), NULLIF(raw_invoices.Mahndatum_3, 0))::INT)) AS second_dunning_date,
        NULLIF(DATE '1801-01-01' + to_days(NULLIF(raw_invoices.Mahndatum_3, 0)::INT), NULLIF(DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_invoices.Mahndatum_2, 0), NULLIF(raw_invoices.Mahndatum_3, 0))::INT), DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_invoices.Mahndatum_1, 0), NULLIF(raw_invoices.Mahndatum_2, 0), NULLIF(raw_invoices.Mahndatum_3, 0))::INT))) AS third_dunning_date,

        DATE '1801-01-01' + to_days(NULLIF(raw_invoices.Eingangsdatum, 0)::INT) AS receipt_date,
        DATE '1801-01-01' + to_days(NULLIF(raw_invoices.ChangeDate, 0)::INT) AS updated_at_date,
        raw_invoices.EMP_Nr AS recipient_id,
        raw_invoices.Art AS invoice_recipient_type,
        NULLIF(raw_invoices.KAS_IK, '') AS ik_number,
        raw_invoices.Name AS recipient_name,
        CASE raw_invoices.Land WHEN 'D' THEN 'DE' ELSE 'unknown' END AS recipient_country_code,
        raw_invoices.PLZ AS recipient_postal_code,
        raw_invoices.Ort AS recipient_city,
        raw_invoices.Zustand AS invoice_status,
        IFNULL(raw_invoices.Versendet::BOOLEAN, FALSE) AS sent,
        raw_invoices.Betrag::DECIMAL(10, 4) AS total_gross,
        raw_invoices.MwSt::DECIMAL(10, 4) AS total_vat,
        NULLIF(raw_invoices.Teilzahlung, 0)::DECIMAL(10, 4) AS partial_payment,
        raw_invoices.Kuerzung::DECIMAL(10, 4) AS deduction,
FROM
        {{ source('starke_praxis', 'raw_invoices') }}

WHERE   1=1
