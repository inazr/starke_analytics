SELECT
        raw_mitarbeiter.Nr AS employee_id,
        raw_mitarbeiter.Kurzname AS employee_short,
        raw_mitarbeiter.Name AS employee_name,
        raw_mitarbeiter.Nachname AS employee_lastname,
        NULLIF(raw_mitarbeiter.Vorname, '') AS employee_firstname,
        NULLIF(raw_mitarbeiter.Geschlecht, '') AS employee_sex,
        raw_mitarbeiter.Ausgeschieden::BOOLEAN AS employee_dismissed,
        DATE '1801-01-01' + to_days(raw_mitarbeiter.ChangeDate::INT) AS updated_at,
FROM
        {{ source('starke_praxis', 'raw_mitarbeiter') }}

WHERE   1=1
