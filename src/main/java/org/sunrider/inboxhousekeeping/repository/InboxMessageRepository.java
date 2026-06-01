package org.sunrider.inboxhousekeeping.repository;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class InboxMessageRepository {

    private static final String ARCHIVED_BY = "inbox-housekeeping-job";

    private final JdbcTemplate jdbcTemplate;

    public List<String> getOldPartitions(int retentionDays) {
        return jdbcTemplate.queryForList("""
            SELECT child.relname AS partition_name
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
            WHERE parent.relname = 'inbox_message'
              AND child.relname != 'inbox_message_default'
              AND TO_DATE(
                    SUBSTRING(child.relname FROM 'inbox_message_(\\d{4}_\\d{2}_\\d{2})'),
                    'YYYY_MM_DD'
                  ) < CURRENT_DATE - INTERVAL '1 day' * ?
            """,
            String.class, retentionDays);
    }

    /**
     * Архивирует ERROR-записи партиции батчами keyset-пагинацией по id.
     * <p>
     * Каждый батч выполняется отдельным стейтментом и, при отсутствии внешней
     * транзакции, отдельной транзакцией — это дробит WAL и не держит долгих
     * локов. Курсор {@code lastId} двигается по окну выборки (CTE {@code batch}),
     * а не по фактически вставленным строкам, поэтому {@code ON CONFLICT DO NOTHING}
     * при повторном прогоне не обрывает цикл преждевременно.
     *
     * @return суммарное число обработанных ERROR-записей
     */
    public int archiveErrorMessages(String partitionName, int batchSize) {
        validatePartitionName(partitionName);
        String sql = """
            WITH batch AS (
                SELECT *
                FROM %s
                WHERE status = 'ERROR'
                  AND id > ?
                ORDER BY id
                LIMIT ?
            ),
            ins AS (
                INSERT INTO archive_inbox_message (
                    id,
                    topic,
                    kafka_partition,
                    kafka_offset,
                    message_key,
                    event_id,
                    event_time,
                    produced_at,
                    status,
                    error_message,
                    created_at,
                    updated_at,
                    created_by,
                    updated_by
                )
                SELECT
                    id,
                    topic,
                    kafka_partition,
                    kafka_offset,
                    message_key,
                    event_id,
                    event_time,
                    produced_at,
                    status,
                    error_message,
                    NOW()    AS created_at,
                    NOW()    AS updated_at,
                    '%2$s'   AS created_by,
                    '%2$s'   AS updated_by
                FROM batch
                ON CONFLICT (id) DO NOTHING
            )
            SELECT max(id) AS last_id, count(*) AS cnt FROM batch
            """.formatted(partitionName, ARCHIVED_BY);

        long lastId = Long.MIN_VALUE;
        int total = 0;
        while (true) {
            Map<String, Object> row = jdbcTemplate.queryForMap(sql, lastId, batchSize);
            int cnt = ((Number) row.get("cnt")).intValue();
            if (cnt == 0) {
                break;
            }
            lastId = ((Number) row.get("last_id")).longValue();
            total += cnt;
            if (cnt < batchSize) {
                break;
            }
        }
        return total;
    }

    /**
     * Возвращает число ERROR-записей партиции, которых ещё нет в архиве (по id).
     * 0 означает, что архивация полна и партицию можно безопасно дропать.
     */
    public int countUnarchivedErrors(String partitionName) {
        validatePartitionName(partitionName);
        Long count = jdbcTemplate.queryForObject("""
            SELECT count(*)
            FROM %s p
            WHERE p.status = 'ERROR'
              AND NOT EXISTS (
                  SELECT 1 FROM archive_inbox_message a WHERE a.id = p.id
              )
            """.formatted(partitionName),
            Long.class);
        return count == null ? 0 : count.intValue();
    }

    public void detachPartition(String partitionName) {
        validatePartitionName(partitionName);
        jdbcTemplate.execute(
            "ALTER TABLE inbox_message DETACH PARTITION %s"
                .formatted(partitionName)
        );
    }

    public void dropPartition(String partitionName) {
        validatePartitionName(partitionName);
        jdbcTemplate.execute(
            "DROP TABLE IF EXISTS %s".formatted(partitionName)
        );
    }

    private void validatePartitionName(String partitionName) {
        if (!partitionName.matches("inbox_message_\\d{4}_\\d{2}_\\d{2}")) {
            throw new IllegalArgumentException("Invalid partition name: " + partitionName);
        }
    }
}
