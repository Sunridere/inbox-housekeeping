package org.sunrider.inboxhousekeeping.repository;

import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.annotation.Timed;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class InboxMessageRepository {

    private static final String ARCHIVED_BY = "inbox-housekeeping-job";

    private final JdbcTemplate jdbcTemplate;

    @Timed("inbox.db.operation")
    @Retry(name = "dbOperation")
    public List<String> getOldPartitions(int retentionDays) {
        return jdbcTemplate.queryForList("""
        SELECT child.relname AS partition_name
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
        WHERE parent.relname = 'inbox_message'
          AND child.relkind = 'r'
          AND child.relname ~ '^inbox_message_\\d{4}_\\d{2}_\\d{2}$'
          AND (regexp_match(
                 pg_get_expr(child.relpartbound, child.oid),
                 $$TO \\('([^']+)'\\)$$
              ))[1]::timestamptz <= (CURRENT_DATE - INTERVAL '1 day' * ?) AT TIME ZONE 'UTC'
        ORDER BY partition_name
        """,
            String.class, retentionDays);
    }

    @Timed("inbox.db.operation")
    @Retry(name = "dbOperation")
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
                    FROM batch b
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM archive_inbox_message a
                        WHERE a.topic = b.topic
                          AND a.kafka_partition = b.kafka_partition
                          AND a.kafka_offset = b.kafka_offset
                    )
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

    @Timed("inbox.db.operation")
    @Retry(name = "dbOperation")
    public int countUnarchivedErrors(String partitionName) {
        validatePartitionName(partitionName);
        Long count = jdbcTemplate.queryForObject("""
            SELECT count(*)
            FROM %s p
            WHERE p.status = 'ERROR'
              AND NOT EXISTS (
                  SELECT 1 
                  FROM archive_inbox_message a 
                  WHERE a.topic = p.topic
                     AND a.kafka_partition = p.kafka_partition
                     AND a.kafka_offset = p.kafka_offset
              )
            """.formatted(partitionName),
            Long.class);
        return count == null ? 0 : count.intValue();
    }

    @Timed("inbox.db.operation")
    @Retry(name = "dbOperation")
    public void dropPartition(String partitionName) {
        validatePartitionName(partitionName);
        jdbcTemplate.execute(
            "DROP TABLE IF EXISTS %s".formatted(partitionName)
        );
    }

    @Timed("inbox.db.operation")
    public Map<String, Long> countByStatusInboxMessage() {
        return jdbcTemplate.query("""
                SELECT status, count(*) as cnt
                FROM inbox_message
                GROUP BY status""",
            (ResultSetExtractor<Map<String, Long>>) rs -> {
                Map<String, Long> result = new HashMap<>();
                while (rs.next()) {
                    result.put(rs.getString("status"), rs.getLong("cnt"));
                }
                return result;
            });
    }

    private void validatePartitionName(String partitionName) {
        if (!partitionName.matches("inbox_message_\\d{4}_\\d{2}_\\d{2}")) {
            throw new IllegalArgumentException("Invalid partition name: " + partitionName);
        }
    }
}
