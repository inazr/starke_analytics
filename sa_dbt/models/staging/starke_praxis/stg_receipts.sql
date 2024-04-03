SELECT
        raw_rezept.Nr AS receipt_id,
        raw_rezept.REC_Nummer AS invoice_number,
        raw_rezept.Euro::BOOLEAN AS is_euro,
        raw_rezept.Art AS receipt_type, -- GKV vs. PKV
        DATE '1801-01-01' + to_days(raw_rezept.Datum::INT) AS receipt_date,
        raw_rezept.Soll AS number_of_treatments,
        raw_rezept.Termine AS number_of_appointments,
        raw_rezept.Ausgefallen AS number_of_cancellations,
        raw_rezept.Verordnung AS prescription_number,
        raw_rezept.Behandlungsbeginn AS treatment_start,
        raw_rezept.Hausbesuch::BOOLEAN AS is_house_call,
        raw_rezept.Heim::BOOLEAN AS is_retirement_home,
        raw_rezept.Bericht AS report,
        raw_rezept.Gruppentherapie::BOOLEAN AS is_group_therapy,
        raw_rezept.Kilometer::NUMERIC(10, 4) AS kilometers,
        raw_rezept.Brutto::NUMERIC(10, 4) AS gross_amount,
        raw_rezept.Zuzahlung::NUMERIC(10, 4) AS supplementary_payment,
        raw_rezept.Pauschale::NUMERIC(10, 4) AS flat_rate,
        raw_rezept.Netto::NUMERIC(10, 4) AS net_amount,
        raw_rezept.MwSt::NUMERIC(10, 4) AS vat,
        raw_rezept.Ausfall::NUMERIC(10, 4) AS cancellation,
        raw_rezept.ZUZ_Status AS supplementary_payment_status,
        raw_rezept.ZUZ_Datum AS supplementary_payment_date,
        raw_rezept.ZUZ_MIT_Kurzname AS supplementary_payment_employee_short,
        raw_rezept.ZUZ_Betrag AS supplementary_payment_amount,
        raw_rezept.ZUZ_Mahnung AS supplementary_payment_reminder,
        raw_rezept.Eigenanteil AS own_share,
        raw_rezept.MIT_Kurzname AS employee_short,
        raw_rezept.KAS_IK AS insurance_ik,
        raw_rezept.KAS_Name AS insurance_name,
        DATE '1801-01-01' + to_days(raw_rezept.ChangeDate::INT) AS updated_at,

FROM
        {{ source('starke_praxis', 'raw_rezept') }}

WHERE   1=1
