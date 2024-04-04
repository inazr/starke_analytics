SELECT
        raw_invoice_line_items.Nr AS invoice_line_item_id,
        raw_invoice_line_items.REC_Nummer AS invoice_number,
        raw_invoice_line_items.REZ_Nr AS receipt_id,
        raw_invoice_line_items.PAT_Name AS patient_name,
        raw_invoice_line_items.PAT_Geburtsjahr AS patient_year_of_birth,
        YEAR(DATE '1801-01-01' + to_days(raw_invoice_line_items.REZ_Datum::INT)) - raw_invoice_line_items.PAT_Geburtsjahr AS patient_age,
        NULLIF(raw_invoice_line_items.PAT_Status, '') AS patient_status,
        DATE '1801-01-01' + to_days(raw_invoice_line_items.Von::INT) AS receipt_from,
        DATE '1801-01-01' + to_days(raw_invoice_line_items.Bis::INT) AS receipt_to,
        raw_invoice_line_items.Text AS text,
        NULLIF(raw_invoice_line_items.Abrechnungscode, '') AS billing_code,
        DATE '1801-01-01' + to_days(raw_invoice_line_items.REZ_Datum::INT) AS invoice_date,
        NULLIF(raw_invoice_line_items.KAS_IK, '') AS ik_number,
        NULLIF(raw_invoice_line_items.KAS_Name, '') AS insurance_name,
        raw_invoice_line_items.TAR_Name AS tariff_name,
        NULLIF(raw_invoice_line_items.DIA_Diagnose, '') AS diagnosis,
        raw_invoice_line_items.Arbeitsunfall::BOOLEAN AS work_accident,
        raw_invoice_line_items.Faktor::NUMERIC(10, 4) AS Faktor,
        raw_invoice_line_items.Anzahl AS number_of_line_items,
        raw_invoice_line_items.Brutto::NUMERIC(10, 4) AS total_gross,
        raw_invoice_line_items.Zuzahlung::NUMERIC(10, 4) AS supplement,
        raw_invoice_line_items.Netto::NUMERIC(10, 4) AS total_net,
        raw_invoice_line_items.Eigenanteil::NUMERIC(10, 4) AS own_share,
        raw_invoice_line_items.Pauschale::NUMERIC(10, 4) AS flat_rate, -- Even though this is stored on the line item it only needs to be accounted in the total_net to the insurance once per receipt!

FROM
        {{ source('starke_praxis', 'raw_invoice_line_items') }}
