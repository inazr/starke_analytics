SELECT
        stg_invoices.invoice_id,
        stg_invoices.invoice_number,
        CASE WHEN stg_invoices.cancellation_invoice_number IS NOT NULL THEN TRUE ELSE FALSE END is_cancellation,
        stg_invoices.cancellation_invoice_number,
        stg_invoices.receipt_id,
        stg_invoices.invoice_date,
        stg_invoices.due_date,
        stg_invoices.first_dunning_date,
        stg_invoices.second_dunning_date,
        stg_invoices.third_dunning_date,
        stg_invoices.invoice_recipient_type,
        stg_invoices.recipient_name,
        stg_invoices.invoice_status,
        stg_invoices.total_gross,
        stg_invoices.partial_payment,
        stg_invoices.deduction,
        stg_invoice_line_items.receipt_id,
        stg_invoice_line_items.patient_name,
        stg_invoice_line_items.patient_year_of_birth,
        stg_invoice_line_items.patient_age,
        stg_invoice_line_items.insurance_name,
        stg_invoice_line_items.total_gross,
        stg_invoice_line_items.supplement,
        stg_invoice_line_items.total_net,
        stg_invoice_line_items.own_share,
        stg_invoice_line_items.flat_rate
FROM
        {{ ref('stg_invoices') }}

LEFT JOIN
        {{ ref('stg_invoice_line_items') }}
   ON   stg_invoices.invoice_number = stg_invoice_line_items.invoice_number

WHERE   1=1

ORDER BY stg_invoices.invoice_id DESC,
         stg_invoice_line_items.invoice_line_item_id DESC
