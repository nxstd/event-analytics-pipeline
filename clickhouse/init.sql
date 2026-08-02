CREATE TABLE IF NOT EXISTS events
(
    event_id UUID,
    type LowCardinality(String),
    amount Decimal(18, 2),
    currency FixedString(3),
    status LowCardinality(String),
    created_at DateTime64(3, 'UTC'),
    processed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (created_at, event_id);
