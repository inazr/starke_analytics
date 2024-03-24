SELECT
        raw_termine.Nr AS termin_id,
        raw_termine.REZ_Nr AS rezept_id,
        DATE '1801-01-01' + to_days(raw_termine.Datum::INT) AS termin_date,
        raw_termine.MIT_Kurzname AS employee_short,
        raw_termine.Kennzeichen AS termin_type,
        raw_termine.Ausgefallen::BOOLEAN AS is_cancelled,
        raw_termine.Multi::BOOLEAN AS is_multi,
        NULLIF(raw_termine.Begruendung, '') AS comment,
        DATE '1801-01-01' + to_days(raw_termine.ChangeDate::INT) AS updated_at,
        raw_termine.Brutto::NUMERIC(10,4) AS gross_payment,
        raw_termine.Zuzahlung::NUMERIC(10,4) AS supplementary_payment,
FROM
        {{ source('starke_praxis', 'raw_termine') }}

WHERE   1=1
