SELECT movie_id, count() AS views
FROM default.raw_events FINAL
WHERE event_date = {day:Date32} AND event_type = 'VIEW_STARTED'
GROUP BY movie_id
ORDER BY views DESC, movie_id ASC
LIMIT 10
FORMAT JSONEachRow
