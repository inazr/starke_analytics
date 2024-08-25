
```sql average_days_between_appointments_per_employee
SELECT
        appointment_employee_short,
        CAST(SUM(days_between_first_and_last_appointment) / SUM(appointment_count - 1) AS NUMERIC(7,4)) AS average_days_between_appointments
FROM
        dim_receipts
WHERE   1=1

GROUP BY 
        1


```

<BarChart 
    title="Ø Tage zwischen Therapien"
    data={average_days_between_appointments_per_employee} 
    swapXY=true 
    x=appointment_employee_short
    y=average_days_between_appointments
    series=appointment_employee_short 
    xType=category
    sort=true
/>
