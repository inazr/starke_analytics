SELECT
        raw_rechnung.Id AS rechnungs_id,
        raw_rechnung.Nummer AS invoice_number,
        NULLIF(raw_rechnung.StornoNummer, '') AS cancellation_invoice_number,
        raw_rechnung.Nachberechnung::BOOLEAN AS subsequent_billing,
        raw_rechnung.REZ_Nr AS rezept_id,
        raw_rechnung.SEN_Nr AS sendungs_id,
        DATE '1801-01-01' + to_days(raw_rechnung.Datum::INT) AS invoice_date,
        DATE '1801-01-01' + to_days(raw_rechnung.Von::INT) AS invoice_valid_from,
        DATE '1801-01-01' + to_days(raw_rechnung.Bis::INT) AS invoice_valid_to,
        DATE '1801-01-01' + to_days(raw_rechnung.Faelligkeitsdatum::INT) AS due_date,

        -- Dunning dates
        -- In the raw data there is a second dunning date but no first dunning date, a third dunning date but no second dunning date, etc.
        -- The following code is a workaround to get the correct dunning dates
        DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_rechnung.Mahndatum_1, 0), NULLIF(raw_rechnung.Mahndatum_2, 0), NULLIF(raw_rechnung.Mahndatum_3, 0))::INT) AS first_dunning_date,
        NULLIF(DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_rechnung.Mahndatum_2, 0), NULLIF(raw_rechnung.Mahndatum_3, 0))::INT), DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_rechnung.Mahndatum_1, 0), NULLIF(raw_rechnung.Mahndatum_2, 0), NULLIF(raw_rechnung.Mahndatum_3, 0))::INT)) AS second_dunning_date,
        NULLIF(DATE '1801-01-01' + to_days(NULLIF(raw_rechnung.Mahndatum_3, 0)::INT), NULLIF(DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_rechnung.Mahndatum_2, 0), NULLIF(raw_rechnung.Mahndatum_3, 0))::INT), DATE '1801-01-01' + to_days(COALESCE(NULLIF(raw_rechnung.Mahndatum_1, 0), NULLIF(raw_rechnung.Mahndatum_2, 0), NULLIF(raw_rechnung.Mahndatum_3, 0))::INT))) AS third_dunning_date,

        DATE '1801-01-01' + to_days(NULLIF(raw_rechnung.Eingangsdatum, 0)::INT) AS receipt_date,
        DATE '1801-01-01' + to_days(NULLIF(raw_rechnung.ChangeDate, 0)::INT) AS updated_at_date,
        raw_rechnung.EMP_Nr AS recipient_id,
        raw_rechnung.Art AS invoice_recipient_type,
        NULLIF(raw_rechnung.KAS_IK, '') AS ik_number,
        raw_rechnung.Name AS recipient_name,
        CASE raw_rechnung.Land WHEN 'D' THEN 'DE' ELSE 'unknown' END AS recipient_country_code,
        raw_rechnung.PLZ AS recipient_postal_code,
        raw_rechnung.Ort AS recipient_city,
        raw_rechnung.Zustand AS invoice_status,
        IFNULL(raw_rechnung.Versendet::BOOLEAN, FALSE) AS sent,
        raw_rechnung.Betrag::DECIMAL(10, 4) AS total_gross,
        raw_rechnung.MwSt::DECIMAL(10, 4) AS total_vat,
        NULLIF(raw_rechnung.Teilzahlung, 0)::DECIMAL(10, 4) AS partial_payment,
        raw_rechnung.Kuerzung::DECIMAL(10, 4) AS deduction,
FROM
        {{ source('starke_praxis', 'raw_rechnung') }}

WHERE   1=1
