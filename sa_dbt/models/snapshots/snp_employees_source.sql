-- Input of the snapshot snp_employees. Only the columns whose history we keep: no password or bank data.
-- Needed because the snapshot's updated_at takes a column name, not an expression.
SELECT
        raw_employees.Nr,
        raw_employees.Kurzname,
        raw_employees.Name,
        raw_employees.Geschlecht,
        raw_employees.Geburtsdatum,
        raw_employees.Rechte,
        raw_employees.Ausgeschieden,
        raw_employees.ADR_Nr,
        raw_employees.Qualifikation,
        raw_employees.Arbeitszeit,
        raw_employees.Ort,
        raw_employees.ChangeDate,
        raw_employees.ChangeTime,
        -- ChangeTime counts hundredths of a second since midnight
        DATE '1801-01-01' + to_days(raw_employees.ChangeDate::INT) + to_milliseconds(raw_employees.ChangeTime * 10) AS updated_at,
FROM
        {{ source('starke_praxis', 'raw_employees') }}

WHERE   1=1
