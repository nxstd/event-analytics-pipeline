from prometheus_client import Counter, Histogram

EVENTS_RECEIVED = Counter("events_received_total", "Events accepted by the API")
EVENTS_PUBLISH_ERRORS = Counter("events_publish_errors_total", "Kafka publish failures")
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["method", "endpoint", "status"],
)
EVENTS_PROCESSED = Counter(
    "events_processed_total", "Events inserted into ClickHouse", ["type", "status"]
)
EVENTS_DUPLICATES = Counter("events_duplicates_total", "Duplicate events skipped")
EVENTS_PROCESSING_ERRORS = Counter("events_processing_errors_total", "Worker processing failures")
EVENT_PROCESSING_DURATION = Histogram(
    "event_processing_duration_seconds", "Worker event processing duration"
)
