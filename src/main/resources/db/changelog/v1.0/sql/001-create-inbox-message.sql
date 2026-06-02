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

-- индексы создаются на родительской таблице
-- PostgreSQL автоматически создаст их на всех партициях
CREATE INDEX idx_inbox_message_status
    ON inbox_message (status);

CREATE INDEX idx_inbox_message_created_at
    ON inbox_message (created_at);

CREATE INDEX idx_inbox_message_event_id
    ON inbox_message (event_id);

-- Функция: создать партицию за конкретный день
CREATE OR REPLACE FUNCTION create_inbox_partition(p_date DATE)
    RETURNS VOID LANGUAGE plpgsql AS $body$
DECLARE
    v_partition_name TEXT;
    v_date_from      DATE;
    v_date_to        DATE;
BEGIN
    v_partition_name := 'inbox_message_' || TO_CHAR(p_date, 'YYYY_MM_DD');
    v_date_from      := p_date;
    v_date_to        := p_date + INTERVAL '1 day';

    -- Пропускаем, если партиция уже существует
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_partition_name
          AND n.nspname = current_schema()
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF inbox_message
             FOR VALUES FROM (%L) TO (%L)',
            v_partition_name,
            v_date_from,
            v_date_to
        );

        RAISE NOTICE 'Partition created: %', v_partition_name;
    ELSE
        RAISE NOTICE 'Partition already exists: %', v_partition_name;
    END IF;
END;
$body$;

-- Функция: создать партиции за диапазон дат
CREATE OR REPLACE FUNCTION create_inbox_partitions(
    p_days_back    INT DEFAULT 7,
    p_days_forward INT DEFAULT 7
)
    RETURNS VOID LANGUAGE plpgsql AS $body$
DECLARE
    v_current_date DATE;
BEGIN
    v_current_date := CURRENT_DATE - p_days_back;

    WHILE v_current_date <= CURRENT_DATE + p_days_forward LOOP
            PERFORM create_inbox_partition(v_current_date);
            v_current_date := v_current_date + INTERVAL '1 day';
        END LOOP;
END;
$body$;

-- Первичная инициализация:
SELECT create_inbox_partitions(
               p_days_back    => 7,
               p_days_forward => 7
       );