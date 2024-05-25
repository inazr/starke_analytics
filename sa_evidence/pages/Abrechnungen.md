```sql top_receipts_without_invoice
SELECT
        fct_receipts_to_appointments.receipt_id,
        fct_receipts_to_appointments.receipt_employee_short,
        MAX(fct_receipts_to_appointments.appointment_date) AS last_appointment_date,
        MAX(fct_receipts_to_appointments.number_of_treatments) AS treatments_on_receipt,
        MAX(fct_receipts_to_appointments.number_of_appointments) AS appointments_on_receipt,

FROM
        fct_receipts_to_appointments
WHERE   1=1
  AND   fct_receipts_to_appointments.invoice_number IS NULL
  AND   fct_receipts_to_appointments.receipt_employee_short IS NOT NULL

GROUP BY
        1,2
        
ORDER BY
        MAX(fct_receipts_to_appointments.number_of_appointments) / MAX(fct_receipts_to_appointments.number_of_treatments) DESC
```

<DataTable data={top_receipts_without_invoice}> 
    <Column id=receipt_employee_short title="Mitarbeiter"/> 
	<Column id=last_appointment_date title="Letzter Termin"/> 
	<Column id=treatments_on_receipt title="Therapien auf Verordnung"/> 
	<Column id=appointments_on_receipt title="Therapien durchgeführt"/>
</DataTable>
