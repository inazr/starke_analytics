-- One row per employee version (snapshot snp_employees)
SELECT
        snp_employees.Nr AS employee_id,
        snp_employees.Kurzname AS employee_short,
        snp_employees.Name AS employee_name,
        NULLIF(snp_employees.Geschlecht, '') AS employee_sex,
        snp_employees.Ausgeschieden::BOOLEAN AS employee_dismissed,
        NULLIF(snp_employees.Arbeitszeit, '')::INT AS weekly_working_hours, -- Sollzeit is always empty, Arbeitszeit holds the hours
        snp_employees.updated_at,
        snp_employees.dbt_valid_from AS valid_from,
        snp_employees.dbt_valid_to AS valid_to,
        snp_employees.dbt_valid_to IS NULL AS is_current,
FROM
        {{ ref('snp_employees') }}

WHERE   1=1
