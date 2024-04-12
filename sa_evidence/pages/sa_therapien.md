<BarChart
  data={appointment_gross_value_by_employee_by_month}
  y=appointment_gross_value
  x=appointment_month
  series=appointment_employee_short
  type=stacked
  yFmt=euro2decimal
  title = "Therapiewert pro Monat pro Mitarbeiterin"
/>

## Write in Markdown

Evidence renders markdown files into web pages. This page is:
`[project]/pages/index.md`.

## Run SQL using Code Fences

```sql appointment_gross_value_by_employee_by_month
SELECT
        DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date) AS appointment_month,
        fct_receipts_to_appointments.appointment_employee_short,
        SUM(fct_receipts_to_appointments.appointment_gross_value) AS appointment_gross_value
FROM
        fct_receipts_to_appointments
WHERE   1=1
  AND   NOT fct_receipts_to_appointments.is_cancelled
GROUP BY
        DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date),
        fct_receipts_to_appointments.appointment_employee_short
```


<BarChart 
    data={average_appointment_gross_value_by_employee_by_month} 
    swapXY=true 
    x=appointment_employee_short 
    y=average_appointment_gross_value 
    series=appointment_employee_short 
    xType=category 
    yFmt=euro2decimal
    sort=true
/>

```sql average_appointment_gross_value_by_employee_by_month
SELECT
        fct_receipts_to_appointments.appointment_employee_short,
        AVG(fct_receipts_to_appointments.appointment_gross_value) AS average_appointment_gross_value
FROM
        fct_receipts_to_appointments
WHERE   1=1
  AND   NOT fct_receipts_to_appointments.is_cancelled
  AND   appointment_date >= '2024-01-01'
GROUP BY
        fct_receipts_to_appointments.appointment_employee_short
```
