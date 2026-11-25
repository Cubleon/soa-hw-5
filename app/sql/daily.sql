WITH {day:Date32} AS selected_day
SELECT
    uniqExact(user_id) AS dau,
    count() AS events_processed,
    avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds,
    countIf(event_type = 'VIEW_FINISHED') / nullIf(countIf(event_type = 'VIEW_STARTED'), 0) AS conversion
FROM default.raw_events FINAL
WHERE event_date = selected_day
FORMAT JSONEachRow
