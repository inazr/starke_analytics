<table>
<tr>
<td>

<BigValue 
  title="# Therapien diesen Monat"
  data={number_of_appointments_current_month} 
  value=number_of_appointments_current_month
  comparison=compared_to_previous_month
  comparisonTitle="zum Vormonat"
  comparisonFmt=pct
/>

</td>
<td>

<BigValue 
  title="# Therapien diesen Monat"
  data={number_of_appointments_current_month} 
  value=number_of_appointments_current_month
  comparison=compared_to_previous_month
  comparisonTitle="zum Vormonat"
  comparisonFmt=pct
/>

</td>
<td>

<BigValue 
  title="# Therapien diesen Monat"
  data={number_of_appointments_current_month} 
  value=number_of_appointments_current_month
  comparison=compared_to_previous_month
  comparisonTitle="zum Vormonat"
  comparisonFmt=pct
/>

</td>
</tr>
</table>

<BarChart
  data={number_of_appointments_per_week_this_year}
  y=number_of_appointments_per_week
  x=iso_week
  series=appointment_employee_short
  yFmt=integer
  type=grouped
  title = "# Therapien"
  subtitle = "pro Mitarbeiterin pro Woche"
/>

<BarChart
  data={appointments_per_month_per_employee}
  y=number_of_appointments
  x=appointment_month
  series=appointment_employee_short
  type=stacked
  yFmt=integer
  title = "# Therapien"
  subtitle = "pro Mitarbeiterin pro Monat"
/>




```sql appointments_per_month_per_employee
SELECT
        DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date) AS appointment_month,
        fct_receipts_to_appointments.appointment_employee_short,
        COUNT(fct_receipts_to_appointments.appointment_id) AS number_of_appointments
FROM
        fct_receipts_to_appointments
WHERE   1=1
  AND   NOT fct_receipts_to_appointments.is_cancelled
GROUP BY
        DATE_TRUNC('MONTH', fct_receipts_to_appointments.appointment_date),
        fct_receipts_to_appointments.appointment_employee_short
```



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

```sql number_of_appointments_per_week_this_year
SELECT
        fct_receipts_to_appointments.appointment_employee_short,
        WEEK(fct_receipts_to_appointments.appointment_date) AS iso_week,
        COUNT(fct_receipts_to_appointments.appointment_id) AS number_of_appointments_per_week
FROM
        fct_receipts_to_appointments
WHERE   1=1
  AND   NOT fct_receipts_to_appointments.is_cancelled
  AND   fct_receipts_to_appointments.appointment_date >= DATE_TRUNC('YEAR', CURRENT_DATE)
  
GROUP BY 
        fct_receipts_to_appointments.appointment_employee_short,
        WEEK(fct_receipts_to_appointments.appointment_date)
```
