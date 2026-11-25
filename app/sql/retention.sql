WITH
    {day:Date32} AS selected_day,
    first_view AS
    (
        SELECT user_id
        FROM default.raw_events FINAL
        WHERE event_type = 'VIEW_STARTED' AND event_date <= selected_day
        GROUP BY user_id
        HAVING min(event_date) = selected_day
    ),
    activity AS
    (
        SELECT user_id, groupUniqArray(event_date) AS dates
        FROM default.raw_events FINAL
        WHERE event_date BETWEEN selected_day AND addDays(selected_day, 7)
        GROUP BY user_id
    ),
    cohort AS
    (
        SELECT
            count() AS cohort_size,
            sumForEach(arrayMap(age -> toUInt64(has(activity.dates, addDays(selected_day, age))), range(8))) AS daily_returns
        FROM first_view
        LEFT ANY JOIN activity USING (user_id)
    )
SELECT
    age,
    cohort_size,
    multiIf(
        cohort_size = 0, NULL,
        age = 0, 1.0,
        addDays(selected_day, age) >= {today:Date32}, NULL,
        daily_returns[age + 1] / nullIf(cohort_size, 0)
    ) AS rate
FROM cohort
ARRAY JOIN range(8) AS age
ORDER BY age
FORMAT JSONEachRow
