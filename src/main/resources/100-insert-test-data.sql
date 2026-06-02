-- Тестовые данные для inbox_message.
-- На каждую существующую дневную партицию генерируется по 500 записей.
-- Диапазон: 7 дней назад .. сегодня (8 дней итого).
-- по 500 записей. Распределение статусов: ~20% ERROR, ~20% NEW, ~60% DONE.
-- id и kafka_offset уникальны за счёт смещения base по дням.
INSERT INTO inbox_message (
    id,
    topic,
    kafka_partition,
    kafka_offset,
    message_key,
    event_id,
    event_time,
    produced_at,
    status,
    payload,
    error_message,
    created_at,
    updated_at,
    created_by,
    updated_by
)
SELECT
    d.base + g                                                AS id,
    'orders'                                                  AS topic,
    g % 6                                                     AS kafka_partition,
    d.base + g                                                AS kafka_offset,
    'key-' || (d.base + g)                                    AS message_key,
    'evt-' || (d.base + g)                                    AS event_id,
    d.day + ((g % 86400) * INTERVAL '1 second')               AS event_time,
    d.day + ((g % 86400) * INTERVAL '1 second')               AS produced_at,
    CASE WHEN g % 5 = 0 THEN 'ERROR'
         WHEN g % 5 = 1 THEN 'NEW'
         ELSE 'DONE' END                                      AS status,
    jsonb_build_object('orderId', d.base + g, 'amount', (g * 7) % 1000) AS payload,
    CASE WHEN g % 5 = 0
         THEN 'deserialization failed at offset ' || (d.base + g)
         END                                                  AS error_message,
    d.day + ((g % 86400) * INTERVAL '1 second')               AS created_at,
    d.day + ((g % 86400) * INTERVAL '1 second')               AS updated_at,
    'kafka-consumer'                                          AS created_by,
    'kafka-consumer'                                          AS updated_by
FROM (
    SELECT
        (CURRENT_DATE - (7 - n)::INT)::TIMESTAMPTZ AS day,
        (n + 1) * 1000000                           AS base
    FROM generate_series(0, 7) AS n
) AS d
CROSS JOIN generate_series(1, 500) AS g;
