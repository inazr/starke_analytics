---
title: Willkommen auf Ihrer Praxisübersicht
---

### Monatliche Therapieübersicht


<Grid cols=3>

<BigValue 
  title="# Therapien"
  data={number_of_appointments_current_month} 
  value=number_of_appointments_current_month
  comparison=compared_to_previous_month
  comparisonTitle="zum Vormonat"
  comparisonFmt=pct
/>

<BigValue 
  title="∑ Wert der Therapien"
  data={sum_gross_value_appointments_current_month} 
  value=number_of_appointments_current_month
  comparison=compared_to_previous_month
  comparisonTitle="zum Vormonat"
  comparisonFmt=pct
  fmt=euro2decimal
/>

<BigValue 
  title="∑ Wert der offenen Therapien"
  data={sum_gross_value_open_appointments_without_invoice} 
  value=open_gross_value
    fmt=euro2decimal
/>

</Grid>

### Jährliche Therapieübersicht

<Grid cols=3>

<BigValue 
  title="# Therapien"
  data={number_of_appointments_current_month} 
  value=number_of_appointments_current_month
  comparison=compared_to_previous_month
  comparisonTitle="zum Vormonat"
  comparisonFmt=pct
/>

<BigValue 
  title="∑ Wert der Therapien"
  data={sum_gross_value_appointments_current_month} 
  value=number_of_appointments_current_month
  comparison=compared_to_previous_month
  comparisonTitle="zum Vormonat"
  comparisonFmt=pct
  fmt=euro2decimal
/>

<BigValue 
  title="∑ Wert der offenen Therapien"
  data={sum_gross_value_open_appointments_without_invoice} 
  value=open_gross_value
    fmt=euro2decimal
/>

</Grid>

```sql number_of_appointments_current_month
SELECT
        COUNT(CASE WHEN (DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date) - INTERVAL '1 MONTH') = (DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 MONTH') THEN fct_receipts_to_appointments.appointment_id END) AS number_of_appointments_current_month,
        (ROUND(COUNT(CASE WHEN (DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date) - INTERVAL '1 MONTH') = (DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 MONTH') THEN fct_receipts_to_appointments.appointment_id END)/ COUNT(CASE WHEN (DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date) - INTERVAL '0 MONTH') = (DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 MONTH') THEN fct_receipts_to_appointments.appointment_id END), 2) - 1) AS compared_to_previous_month
FROM
        fct_receipts_to_appointments
WHERE   1=1
  AND   NOT fct_receipts_to_appointments.is_cancelled
  AND   fct_receipts_to_appointments.appointment_date >= (DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 MONTH')
```

```sql sum_gross_value_appointments_current_month
SELECT
        SUM(CASE WHEN (DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date) - INTERVAL '1 MONTH') = (DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 MONTH') THEN fct_receipts_to_appointments.appointment_gross_value END) AS number_of_appointments_current_month,
        (ROUND(SUM(CASE WHEN (DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date) - INTERVAL '1 MONTH') = (DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 MONTH') THEN fct_receipts_to_appointments.appointment_gross_value END)/ SUM(CASE WHEN (DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date) - INTERVAL '0 MONTH') = (DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 MONTH') THEN fct_receipts_to_appointments.appointment_gross_value END), 2) - 1) AS compared_to_previous_month
FROM
        fct_receipts_to_appointments
WHERE   1=1
  AND   NOT fct_receipts_to_appointments.is_cancelled
  AND   fct_receipts_to_appointments.appointment_date >= (DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 MONTH')
  AND   EXTRACT(DATE FROM fct_receipts_to_appointments.appointment_date) <= EXTRACT(DATE FROM CURRENT_DATE)
```

```sql sum_gross_value_open_appointments_without_invoice
SELECT
        SUM(fct_receipts_to_appointments.appointment_gross_value) AS open_gross_value,
FROM
        fct_receipts_to_appointments
WHERE   1=1
  AND   fct_receipts_to_appointments.invoice_number IS NULL
  AND   YEAR(fct_receipts_to_appointments.appointment_date) = YEAR(CURRENT_DATE)
```
