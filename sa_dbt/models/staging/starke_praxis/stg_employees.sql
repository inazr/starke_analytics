SELECT
        raw_employees.Nr AS employee_id,
        raw_employees.Kurzname AS employee_short,
        raw_employees.Name AS employee_name,
        raw_employees.Nachname AS employee_lastname,
        NULLIF(raw_employees.Vorname, '') AS employee_firstname,
        NULLIF(raw_employees.Geschlecht, '') AS employee_sex,
        raw_employees.Ausgeschieden::BOOLEAN AS employee_dismissed,
        DATE '1801-01-01' + to_days(raw_employees.ChangeDate::INT) AS updated_at,
FROM
        {{ source('starke_praxis', 'raw_employees') }}

WHERE   1=1
