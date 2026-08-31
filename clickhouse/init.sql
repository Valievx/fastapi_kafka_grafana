CREATE TABLE IF NOT EXISTS game_events
(
    event_id UUID,
    event_type LowCardinality(String),
    time DateTime64(3),
    text String
)
ENGINE = MergeTree
ORDER BY (time, event_id);