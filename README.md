# ДЗ №5 · Online Cinema Analytics

Movie Service принимает события через HTTP и публикует их в Kafka. ClickHouse читает Avro напрямую через Kafka Engine и сохраняет события в `raw_events`. Aggregation Service рассчитывает метрики в ClickHouse, обновляет дневные срезы в ClickHouse и PostgreSQL и выгружает закрытые UTC-дни из PostgreSQL в S3. Grafana читает дневные срезы ClickHouse.

## Запуск

Запустите стенд из каталога `HW5`.

```bash
docker compose up
```

Bootstrap автоматически создаёт топик и бакет, регистрирует схему в Karapace Schema Registry и применяет DDL ClickHouse и PostgreSQL. Статус штатного завершения bootstrap — `Exited (0)`. Сервисы, которые работают постоянно, имеют health checks. Docker Compose хранит данные в именованных томах.

| Компонент | Адрес | Локальный вход |
|---|---|---|
| Movie Service | [Swagger](http://localhost:8000/docs) | — |
| Aggregation Service | [Swagger](http://localhost:8002/docs) | — |
| Schema Registry | [Avro v1](http://localhost:8081/subjects/movie-events-value/versions/1) | — |
| Grafana | [Online Cinema Analytics](http://localhost:3000/d/movie-analytics) | `admin` / `cinema-local` |
| ClickHouse | `http://localhost:8123` | `analytics` / `analytics-local` |
| SeaweedFS S3 | `http://localhost:8333` | `analytics` / `analytics-local` |

Генератор по умолчанию загружает 14 завершённых дней и каждые 6 секунд создаёт новую сессию. В исторических сессиях идентификаторы и данные воспроизводимы. На повторном старте такие события объединяются ClickHouse по ключу. Настройки можно передать при запуске Compose или через `.env`.

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `GENERATOR_ENABLED` | `true` | Включить синтетические сессии |
| `GENERATOR_INTERVAL_SECONDS` | `6` | Пауза между новыми сессиями |
| `GENERATOR_HISTORY_DAYS` | `14` | Глубина стартовой загрузки |
| `AGGREGATION_INTERVAL_SECONDS` | `30` | Пауза между циклами агрегации и экспорта |

## API

Публикация события. Подставьте дату и время события с явным смещением или `Z`. При успехе API отвечает `201` и возвращает `event_id` после подтверждения Kafka. Ошибки данных возвращают `422`, ошибки публикации — `503`. При повторной отправке того же события сохраните исходный UUID и значения полей.

```bash
curl -fsS -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "4443c814-b938-469a-baf8-43d946228267",
    "user_id": "demo-viewer",
    "movie_id": "aurora",
    "event_type": "VIEW_STARTED",
    "timestamp": "2026-11-25T12:00:00.123Z",
    "device_type": "TV",
    "session_id": "demo-session",
    "progress_seconds": 0
  }'
```

Для ручного пересчёта и повторной выгрузки укажите дату в UTC.

```bash
DAY=2026-11-25
curl -fsS -X POST "http://localhost:8002/recompute/$DAY"
curl -fsS -X POST "http://localhost:8002/export/$DAY"
```

У экспорта стабильный ключ `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`. Каждая запись содержит дату, название метрики, измерение, значение и время вычисления. Повторная выгрузка перезаписывает объект. Установленный AWS CLI позволяет прочитать выгрузку из бакета.

```bash
AWS_ACCESS_KEY_ID=analytics AWS_SECRET_ACCESS_KEY=analytics-local \
  aws --endpoint-url http://localhost:8333 s3 cp \
  "s3://movie-analytics/daily/$DAY/aggregates.json" -
```

## Данные и метрики

- [Схема Avro](schemas/movie-event-v1.avsc) использует UUID, UTC с точностью до миллисекунд, два перечисления и nullable `progress_seconds`. Для `VIEW_*` API требует целое неотрицательное значение прогресса. Subject `movie-events-value` регистрируется как версия 1 с режимом совместимости `BACKWARD_TRANSITIVE`.
- Ключ Kafka — `user_id`. Kafka направляет события одного пользователя в одну партицию и сохраняет порядок публикации, в том числе при переходе пользователя между фильмами. Идемпотентный продюсер использует `acks=all` и повторные попытки с экспоненциальной задержкой.
- [ClickHouse](app/sql/001_clickhouse.sql) хранит raw-события в `ReplacingMergeTree`, с партициями по месяцу UTC и сортировкой по дате, пользователю, времени и UUID. Сортировка помогает отбирать дни и читать активность пользователей. Запросы метрик используют `FINAL`, чтобы повторы идентичного события при доставке учитывались один раз до завершения фоновых слияний.
- DAU — точное число уникальных пользователей с любым событием за день. Среднее время — среднее значение `progress_seconds` по `VIEW_FINISHED`. Топ за день — десять фильмов по числу `VIEW_STARTED`, с сортировкой по `movie_id` при равном количестве. Конверсия — отношение числа `VIEW_FINISHED` к числу `VIEW_STARTED` за день. При переходах просмотров через полночь отношение может превышать 100%.
- Когорта начинается в день первого `VIEW_STARTED` пользователя. Пользователь считается вернувшимся, если совершил любое действие в нужный UTC-день. [Запрос retention](app/sql/retention.sql) считает Day 0–7, включая D1 и D7. Day 0 равен 100% для непустой когорты. Пока соответствующий день возврата не закрылся, его значение равно `NULL`.
- Сервис атомарно заменяет дневную партицию агрегатов в ClickHouse. В PostgreSQL один срез хранится в одной транзакции по ключу `(metric_date, metric, dimension)`. Блокировка PostgreSQL по дате сериализует пересчёт и экспорт одного среза. Повторный цикл догоняет незавершённые записи, а версии, ожидающие S3, хранятся в PostgreSQL.
- Планировщик проверяет сегодня, вчера, недавние когорты и исторические дни, в которых появились события. Изменения активности также обновляют затронутые когорты. Когда закрывается последний день для D7, обновлённый срез выгружается по тому же пути. Любую дату можно пересчитать вручную.

## Проверка

Команда запускает тесты и при необходимости поднимает зависимости.

```bash
docker compose run --build --rm integration
```

Тест с уникальными идентификаторами и отдельной исторической датой посылает события через HTTP, ждёт их в ClickHouse и сравнивает все поля. Он также проверяет повторный пересчёт, DAU, длительность, топ, конверсию, все дни когорты, содержимое S3 и запросы панелей через Grafana.

Три узла Kafka работают в режиме KRaft. Для `movie-events` заданы три партиции, `replication.factor=2` и `min.insync.replicas=1`. Кворум из трёх контроллеров выдерживает остановку одного узла. Для ручной демонстрации остановите `kafka-1`, отправьте через API несколько событий с новыми UUID, проверьте их в ClickHouse и верните узел в кластер.

```bash
docker compose stop kafka-1
# После перевыборов Kafka отправьте события через API.
docker compose start kafka-1
```
