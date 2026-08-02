#!/usr/bin/env python3
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime

API_URL = "http://localhost:8000"
CLICKHOUSE_URL = "http://localhost:8123"
TIMEOUT_SECONDS = 60
EVENT_TYPE = "transaction.completed"
STATUS = "success"
CURRENCY = "RUB"
AMOUNT = 1490.50


def request_json(path: str, *, method: str = "GET", body: dict | None = None) -> dict:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        f"{API_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def wait_until_ready() -> None:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            response = request_json("/health/ready")
            if response.get("status") == "ready":
                return
        except (OSError, urllib.error.HTTPError, json.JSONDecodeError):
            pass
        time.sleep(1)
    raise TimeoutError("API did not become ready")


def group_count(stats: dict) -> int:
    for group in stats.get("groups", []):
        if (
            group.get("type") == EVENT_TYPE
            and group.get("status") == STATUS
            and group.get("currency") == CURRENCY
        ):
            return int(group["events_count"])
    return 0


def wait_for_count_greater_than(initial_count: int) -> tuple[int, dict]:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        stats = request_json("/stats")
        current_count = group_count(stats)
        if current_count > initial_count:
            return current_count, stats
        time.sleep(1)
    raise TimeoutError("Event did not appear in /stats")


def clickhouse_event_count(event_id: str) -> int:
    query = f"SELECT count() FROM events WHERE event_id = toUUID('{event_id}')"
    url = f"{CLICKHOUSE_URL}/?{urllib.parse.urlencode({'query': query})}"
    with urllib.request.urlopen(url, timeout=5) as response:
        raw_count = response.read().decode("utf-8").strip()
    try:
        return int(raw_count)
    except ValueError as exc:
        raise AssertionError(f"Unexpected ClickHouse count response: {raw_count!r}") from exc


def main() -> int:
    wait_until_ready()
    initial_count = group_count(request_json("/stats"))
    event_id = str(uuid.uuid4())
    event = {
        "event_id": event_id,
        "type": EVENT_TYPE,
        "amount": AMOUNT,
        "currency": CURRENCY,
        "status": STATUS,
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }

    accepted = request_json("/events", method="POST", body=event)
    if accepted != {"event_id": event_id, "status": "accepted"}:
        raise AssertionError(f"Unexpected POST response: {accepted}")

    processed_count, stats = wait_for_count_greater_than(initial_count)
    request_json("/events", method="POST", body=event)
    time.sleep(3)
    duplicate_count = group_count(request_json("/stats"))
    if duplicate_count != processed_count:
        raise AssertionError(
            f"Duplicate changed the aggregate: {processed_count} -> {duplicate_count}"
        )

    physical_count = clickhouse_event_count(event_id)
    if physical_count != 1:
        raise AssertionError(
            f"ClickHouse contains {physical_count} rows for event_id={event_id}; expected 1"
        )

    print(f"Direct ClickHouse row count for event_id={event_id}: {physical_count}")
    print(f"Smoke test passed for event_id={event_id}")
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Smoke test failed: {exc}", file=sys.stderr)
        sys.exit(1)
