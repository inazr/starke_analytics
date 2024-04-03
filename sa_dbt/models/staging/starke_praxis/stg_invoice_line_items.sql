SELECT
        raw_rechnzeile.Nr AS invoice_line_item_id,
        raw_rechnzeile.REC_Nummer AS invoice_number,
        raw_rechnzeile.REZ_Nr AS receipt_id,
        raw_rechnzeile.PAT_Name AS patient_name,
        raw_rechnzeile.PAT_Geburtsjahr AS patient_year_of_birth,
        YEAR(DATE '1801-01-01' + to_days(raw_rechnzeile.REZ_Datum::INT)) - raw_rechnzeile.PAT_Geburtsjahr AS patient_age,
        NULLIF(raw_rechnzeile.PAT_Status, '') AS patient_status,
        DATE '1801-01-01' + to_days(raw_rechnzeile.Von::INT) AS receipt_from,
        DATE '1801-01-01' + to_days(raw_rechnzeile.Bis::INT) AS receipt_to,
        raw_rechnzeile.Text AS text,
        NULLIF(raw_rechnzeile.Abrechnungscode, '') AS billing_code,
        DATE '1801-01-01' + to_days(raw_rechnzeile.REZ_Datum::INT) AS invoice_date,
        NULLIF(raw_rechnzeile.KAS_IK, '') AS ik_number,
        NULLIF(raw_rechnzeile.KAS_Name, '') AS insurance_name,
        raw_rechnzeile.TAR_Name AS tariff_name,
        NULLIF(raw_rechnzeile.DIA_Diagnose, '') AS diagnosis,
        raw_rechnzeile.Arbeitsunfall::BOOLEAN AS work_accident,
        raw_rechnzeile.Faktor::NUMERIC(10, 4) AS Faktor,
        raw_rechnzeile.Anzahl AS number_of_line_items,
        raw_rechnzeile.Brutto::NUMERIC(10, 4) AS total_gross,
        raw_rechnzeile.Zuzahlung::NUMERIC(10, 4) AS supplement,
        raw_rechnzeile.Netto::NUMERIC(10, 4) AS total_net,
        raw_rechnzeile.Eigenanteil::NUMERIC(10, 4) AS own_share,
        raw_rechnzeile.Pauschale::NUMERIC(10, 4) AS flat_rate, -- Even though this is stored on the line item it only needs to be accounted in the total_net to the insurance once per receipt!

FROM
        {{ source('starke_praxis', 'raw_rechnzeile') }}
