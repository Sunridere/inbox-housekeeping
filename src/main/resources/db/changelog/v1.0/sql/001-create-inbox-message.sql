CREATE TABLE inbox_message
(
    id              BIGINT      NOT NULL,
    topic           TEXT        NOT NULL,
    kafka_partition INT         NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    message_key     TEXT,
    event_id        TEXT        NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    produced_at     TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL,
    payload         JSONB,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      TEXT        NOT NULL,
    updated_by      TEXT        NOT NULL,

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE inbox_message_default
    PARTITION OF inbox_message DEFAULT;

CREATE TABLE inbox_message_2026_05_27
    PARTITION OF inbox_message
    FOR VALUES FROM ('2026-05-27') TO ('2026-05-28');

CREATE TABLE inbox_message_2026_05_28
    PARTITION OF inbox_message
    FOR VALUES FROM ('2026-05-28') TO ('2026-05-29');

CREATE TABLE inbox_message_2026_05_29
    PARTITION OF inbox_message
    FOR VALUES FROM ('2026-05-29') TO ('2026-05-30');

CREATE TABLE inbox_message_2026_05_30
    PARTITION OF inbox_message
    FOR VALUES FROM ('2026-05-30') TO ('2026-05-31');

CREATE TABLE inbox_message_2026_05_31
    PARTITION OF inbox_message
    FOR VALUES FROM ('2026-05-31') TO ('2026-06-01');

CREATE TABLE inbox_message_2026_06_01
    PARTITION OF inbox_message
    FOR VALUES FROM ('2026-06-01') TO ('2026-06-02');

CREATE TABLE inbox_message_2026_06_02
    PARTITION OF inbox_message
    FOR VALUES FROM ('2026-06-02') TO ('2026-06-03');

-- индексы создаются на родительской таблице
-- PostgreSQL автоматически создаст их на всех партициях
CREATE INDEX idx_inbox_message_status
    ON inbox_message (status);

CREATE INDEX idx_inbox_message_created_at
    ON inbox_message (created_at);

CREATE INDEX idx_inbox_message_event_id
    ON inbox_message (event_id);