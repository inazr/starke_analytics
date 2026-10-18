SELECT
        employee_payments.month::DATE AS payment_month,
        employee_payments.employee_short::VARCHAR AS employee_short,
        employee_payments.actual_salary::NUMERIC(10,4) AS actual_salary,
FROM
        {{ ref('employee_payments') }}

WHERE   1=1
