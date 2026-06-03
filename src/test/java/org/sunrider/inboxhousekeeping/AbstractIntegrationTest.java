package org.sunrider.inboxhousekeeping;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest
@ActiveProfiles("test")
public abstract class AbstractIntegrationTest {

    private static final DateTimeFormatter PARTITION_DATE = DateTimeFormatter.ofPattern("yyyy_MM_dd");

    private static final AtomicLong ID_SEQ = new AtomicLong(0);

    private static final String INSERT_SQL = """
        INSERT INTO inbox_message
            (id, topic, kafka_partition, kafka_offset, message_key, event_id,
             event_time, produced_at, status, payload, error_message,
             created_at, updated_at, created_by, updated_by)
        VALUES (?, 'orders', 0, ?, ?, ?, ?, ?, ?, '{}'::jsonb, ?, ?, ?, 'test', 'test')
        """;

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine")
            .withEnv("TZ", "UTC")
            .withEnv("PGTZ", "UTC");

    static {
        POSTGRES.start();
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        List<String> partitions = jdbc.queryForList("""
            SELECT child.relname
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
            WHERE parent.relname = 'inbox_message'
              AND child.relname <> 'inbox_message_default'
            """, String.class);

        jdbc.execute("TRUNCATE TABLE inbox_message, archive_inbox_message, shedlock");

        for (String partition : partitions) {
            jdbc.execute("DROP TABLE IF EXISTS " + partition);
        }
    }

    /** Имя дневной партиции для даты, напр. inbox_message_2026_05_24. */
    protected String partitionName(LocalDate day) {
        return "inbox_message_" + day.format(PARTITION_DATE);
    }

    /** Создаёт дневную партицию через ту же plpgsql-функцию, что и прод (идентичный relpartbound). */
    protected String createPartition(LocalDate day) {
        jdbc.execute("SELECT create_inbox_partition('" + day + "'::date)");
        return partitionName(day);
    }

    /** Вставляет одну запись с created_at в полдень указанного дня (попадёт в партицию этого дня). */
    protected long insertMessage(LocalDate day, String status) {
        long id = ID_SEQ.incrementAndGet();
        OffsetDateTime ts = day.atTime(12, 0).atOffset(ZoneOffset.UTC);
        String errorMessage = "ERROR".equals(status) ? "boom" : null;
        jdbc.update(INSERT_SQL,
            id, id, "key-" + id, "evt-" + id, ts, ts, status, errorMessage, ts, ts);
        return id;
    }

    protected void insertMessages(LocalDate day, String status, int count) {
        for (int i = 0; i < count; i++) {
            insertMessage(day, status);
        }
    }

    protected int countRows(String table) {
        Integer count = jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class);
        return count == null ? 0 : count;
    }

    protected boolean partitionExists(String name) {
        return Boolean.TRUE.equals(jdbc.queryForObject(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = ?)", Boolean.class, name));
    }

    protected LocalDate today() {
        return LocalDate.now(ZoneOffset.UTC);
    }
}
