SELECT
        raw_appointments.Nr AS appointment_id,
        raw_appointments.REZ_Nr AS receipt_id,
        CASE WHEN raw_appointments.Datum < 70000
             THEN NULL
             ELSE DATE '1801-01-01' + to_days(raw_appointments.Datum::INT)
        END AS appointment_date,
        raw_appointments.MIT_Kurzname AS employee_short,
        raw_appointments.Kennzeichen AS appointment_type,
        raw_appointments.Ausgefallen::BOOLEAN AS is_cancelled,
        raw_appointments.Multi::BOOLEAN AS is_multi,
        NULLIF(raw_appointments.Begruendung, '') AS comment,
        DATE '1801-01-01' + to_days(raw_appointments.ChangeDate::INT) AS updated_at,
        raw_appointments.Brutto::NUMERIC(10,4) AS gross_value,
        raw_appointments.Zuzahlung::NUMERIC(10,4) AS supplementary_value,
FROM
        {{ source('starke_praxis', 'raw_appointments') }}

WHERE   1=1
