SELECT
        stg_receipts.receipt_id,
        stg_appointments.appointment_id,
        stg_receipts.invoice_number,
        stg_receipts.receipt_date,
        stg_appointments.appointment_date,
        stg_receipts.employee_short AS receipt_employee_short,
        stg_appointments.employee_short AS appointment_employee_short,
        stg_receipts.insurance_name,
        stg_receipts.receipt_type,
        stg_receipts.number_of_treatments,
        stg_receipts.number_of_appointments,
        stg_receipts.number_of_cancellations,
        CASE WHEN stg_receipts.invoice_number IS NOT NULL
              AND stg_receipts.number_of_appointments < stg_receipts.number_of_treatments
             THEN TRUE
             ELSE FALSE
        END AS is_receipt_incomplete,
        stg_appointments.appointment_type,
        stg_appointments.is_cancelled,
        stg_receipts.is_house_call,
        stg_receipts.is_retirement_home,
        stg_appointments.gross_value AS appointment_gross_value,
        stg_appointments.supplementary_value AS appointment_supplementary_value,
        stg_receipts.gross_amount AS receipt_gross_value,
        stg_receipts.net_amount AS receipt_net_value,
        stg_receipts.supplementary_payment AS receipt_supplementary_value,
        stg_receipts.flat_rate AS receipt_flat_rate,
FROM
        {{ ref('stg_receipts') }}

LEFT JOIN
        {{ ref('stg_appointments') }}
   ON   stg_receipts.receipt_id = stg_appointments.receipt_id

WHERE   1=1

ORDER BY
        stg_receipts.receipt_id,
        stg_appointments.appointment_date ASC NULLS FIRST
