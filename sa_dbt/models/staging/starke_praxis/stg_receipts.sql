SELECT
        raw_receipts.Nr AS receipt_id,
        NULLIF(raw_receipts.REC_Nummer, '') AS invoice_number,
        raw_receipts.Euro::BOOLEAN AS is_euro,
        raw_receipts.Art AS receipt_type, -- GKV vs. PKV
        DATE '1801-01-01' + to_days(raw_receipts.Datum::INT) AS receipt_date,
        raw_receipts.Soll AS number_of_treatments,
        raw_receipts.Termine AS number_of_appointments,
        raw_receipts.Ausgefallen AS number_of_cancellations,
        raw_receipts.Verordnung AS prescription_number,
        DATE '1801-01-01' + to_days(raw_receipts.Behandlungsbeginn::INT) AS treatment_start,
        raw_receipts.Hausbesuch::BOOLEAN AS is_house_call,
        raw_receipts.Heim::BOOLEAN AS is_retirement_home,
        raw_receipts.Bericht AS report,
        raw_receipts.Gruppentherapie::BOOLEAN AS is_group_therapy,
        raw_receipts.Kilometer::NUMERIC(10, 4) AS kilometers,
        raw_receipts.Brutto::NUMERIC(10, 4) AS gross_amount,
        raw_receipts.Zuzahlung::NUMERIC(10, 4) AS supplementary_payment,
        raw_receipts.Pauschale::NUMERIC(10, 4) AS flat_rate,
        raw_receipts.Netto::NUMERIC(10, 4) AS net_amount,
        raw_receipts.MwSt::NUMERIC(10, 4) AS vat,
        raw_receipts.Ausfall::NUMERIC(10, 4) AS cancellation,
        raw_receipts.ZUZ_Status AS supplementary_payment_status,
        DATE '1801-01-01' + to_days(raw_receipts.ZUZ_Datum::INT) AS supplementary_payment_date,
        raw_receipts.ZUZ_MIT_Kurzname AS supplementary_payment_employee_short,
        raw_receipts.ZUZ_Betrag AS supplementary_payment_amount,
        raw_receipts.ZUZ_Mahnung AS supplementary_payment_reminder,
        raw_receipts.Eigenanteil AS own_share,
        raw_receipts.MIT_Kurzname AS employee_short,
        NULLIF(raw_receipts.KAS_IK, '') AS insurance_ik,
        NULLIF(raw_receipts.KAS_Name, '') AS insurance_name,
        raw_receipts.PAT_Name AS patient_name, -- hashed during extraction, same hash as on the invoice line items
        raw_receipts.ARZ_Name AS doctor_name, -- prescribing doctor, hashed during extraction
        NULLIF(raw_receipts.Diagnosetext, '') AS diagnosis_text,
        raw_receipts.Heilmittelbereich::INT AS therapy_area,
        -- Prescribed frequency: FrequenzVon to FrequenzBis sessions per FrequenzEinheit (W = week); 0 = not set
        NULLIF(raw_receipts.FrequenzVon, 0) AS frequency_min,
        NULLIF(raw_receipts.FrequenzBis, 0) AS frequency_max,
        NULLIF(raw_receipts.FrequenzEinheit, '') AS frequency_unit,
        -- Starke's own checks of the prescription rules
        raw_receipts.FristOK::BOOLEAN AS is_deadline_ok,
        raw_receipts.BeginnOK::BOOLEAN AS is_start_ok,
        raw_receipts.Dringlich::BOOLEAN AS is_urgent,
        DATE '1801-01-01' + to_days(raw_receipts.ChangeDate::INT) AS updated_at,

FROM
        {{ source('starke_praxis', 'raw_receipts') }}

WHERE   1=1
