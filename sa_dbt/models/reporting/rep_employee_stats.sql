-- One row per month and employee: done therapy sessions, their revenue and the salary paid.
-- Revenue = gross value of the appointments (matches the receipt totals); cancelled and future appointments don't count.
WITH appointments_per_month AS (
    SELECT
            DATE_TRUNC('month', fct_receipts_to_appointments.appointment_date)::DATE AS report_month,
            fct_receipts_to_appointments.appointment_employee_short AS employee_short,
            COUNT(*) AS appointments_count,
            SUM(fct_receipts_to_appointments.appointment_gross_value) AS revenue_gross,
    FROM
            {{ ref('fct_receipts_to_appointments') }}

    WHERE   1=1
      AND   fct_receipts_to_appointments.appointment_employee_short IS NOT NULL
      AND   NOT fct_receipts_to_appointments.is_cancelled
      AND   fct_receipts_to_appointments.appointment_date <= current_date

    GROUP BY
            1,2
)

SELECT
        COALESCE(appointments_per_month.report_month, stg_employee_payments.payment_month) AS report_month,
        COALESCE(appointments_per_month.employee_short, stg_employee_payments.employee_short) AS employee_short,
        stg_employee_payments.actual_salary,
        IFNULL(appointments_per_month.appointments_count, 0) AS appointments_count,
        IFNULL(appointments_per_month.revenue_gross, 0)::NUMERIC(10,4) AS revenue_gross,
FROM
        appointments_per_month

FULL OUTER JOIN
        {{ ref('stg_employee_payments') }}
   ON   appointments_per_month.report_month = stg_employee_payments.payment_month
  AND   appointments_per_month.employee_short = stg_employee_payments.employee_short

WHERE   1=1
