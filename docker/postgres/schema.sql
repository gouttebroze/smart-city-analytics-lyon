
CREATE TABLE IF NOT EXISTS raw.traffic_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    event_type TEXT NOT NULL,
    source TEXT NOT NULL,

    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_timestamp TIMESTAMPTZ,

    payload JSONB NOT NULL,

    geom geometry(Point, 4326),

    CONSTRAINT uq_traffic_kafka_message
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_traffic_events_collected_at
    ON raw.traffic_events (collected_at);

CREATE INDEX IF NOT EXISTS idx_traffic_events_geom
    ON raw.traffic_events
    USING GIST (geom);
