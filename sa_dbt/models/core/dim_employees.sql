SELECT
        stg_employees.employee_id,
        stg_employees.employee_short,
        stg_employees.employee_dismissed,
        stg_employees.weekly_working_hours,
        stg_employees.valid_from,
        stg_employees.valid_to,
        stg_employees.is_current,
FROM
        {{ ref('stg_employees') }}

WHERE   1=1
