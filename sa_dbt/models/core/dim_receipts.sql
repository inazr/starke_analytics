SELECT
        fct_receipts_to_appointments.receipt_id,
        fct_receipts_to_appointments.appointment_employee_short,
        fct_receipts_to_appointments.number_of_treatments,
        fct_receipts_to_appointments.number_of_appointments,
        COUNT(fct_receipts_to_appointments.appointment_date) AS appointment_count,
        MIN(fct_receipts_to_appointments.appointment_date) AS first_appointment_date,
        MAX(fct_receipts_to_appointments.appointment_date) AS last_appointment_date,
        DATE_DIFF('day', first_appointment_date, last_appointment_date) AS days_between_first_and_last_appointment,
        CAST(days_between_first_and_last_appointment / appointment_count AS NUMERIC(7,4)) AS average_days_between_appointments,
FROM
        {{ ref('fct_receipts_to_appointments') }}

WHERE   1=1
  AND   appointment_employee_short IS NOT NULL
  AND   NOT is_cancelled

GROUP BY
        1,2,3,4
