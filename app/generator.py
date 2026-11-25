import logging
import os
from datetime import UTC, datetime, time, timedelta
from random import Random
from uuid import NAMESPACE_URL, uuid4, uuid5

from app.common import utc_now
from app.events import DeviceType, EventType, MovieEvent

log = logging.getLogger(__name__)
MOVIES = ("aurora", "afterglow", "silence", "orbit", "wild-river", "long-road")
DURATIONS = (5520, 6000, 6840, 6360, 5100, 5760)


def view_session(random, user, start, session_id, historical):
    movie_index = random.choices(range(len(MOVIES)), weights=(30, 20, 17, 14, 10, 9))[0]
    device = random.choice(list(DeviceType))
    duration = DURATIONS[movie_index]

    timeline = []
    if random.random() < 0.8:
        timeline.append((EventType.SEARCHED, -25, None))
    timeline.append((EventType.VIEW_STARTED, 0, 0))
    pause_at = random.randint(180, 1800)
    pause_length = 0
    if random.random() < 0.55:
        timeline.append((EventType.VIEW_PAUSED, pause_at, pause_at))
        pause_length = random.randint(30, 300)
        timeline.append((EventType.VIEW_RESUMED, pause_at + pause_length + 1, pause_at + 1))
    if random.random() < 0.7:
        timeline.append((EventType.VIEW_FINISHED, duration + pause_length, duration))
        if random.random() < 0.35:
            timeline.append((EventType.LIKED, duration + pause_length + 20, None))

    for kind, elapsed, progress in timeline:
        yield MovieEvent(
            event_id=uuid5(NAMESPACE_URL, f"{session_id}/{kind}") if historical else uuid4(),
            user_id=user,
            movie_id=MOVIES[movie_index],
            event_type=kind,
            timestamp=start + timedelta(seconds=elapsed),
            device_type=device,
            session_id=session_id,
            progress_seconds=progress,
        )


def historical_sessions(today, days):
    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        for age in range(14):
            cohort = day - timedelta(days=age)
            if cohort < today - timedelta(days=days):
                break
            for index in range(8):
                user = f"demo-{cohort}-{index:02}"
                session_id = f"{user}-{day}"
                random = Random(session_id)
                if age and random.random() >= 0.64 / age**0.55:
                    continue
                start = datetime.combine(day, time(random.randrange(8, 21), random.randrange(60)), UTC)
                yield view_session(random, user, start, session_id, historical=True)


def run_generator(publish, stop):
    backfill_days = max(0, int(os.getenv("GENERATOR_HISTORY_DAYS", "14")))
    interval = max(1, float(os.getenv("GENERATOR_INTERVAL_SECONDS", "6")))
    random = Random()

    def publish_sessions(sessions):
        for session in sessions:
            for event in session:
                while not stop.is_set():
                    try:
                        publish(event)
                        break
                    except Exception:
                        log.exception("Generator could not publish %s; retrying", event.event_id)
                        stop.wait(interval)
                if stop.is_set():
                    return

    log.info("Generating %s days of demo history", backfill_days)
    publish_sessions(historical_sessions(utc_now().date(), backfill_days))
    while not stop.is_set():
        user = f"demo-live-{random.randrange(40):02}"
        start = utc_now() - timedelta(hours=2)
        session = view_session(random, user, start, f"{user}-{uuid4()}", historical=False)
        publish_sessions([session])
        stop.wait(interval)
