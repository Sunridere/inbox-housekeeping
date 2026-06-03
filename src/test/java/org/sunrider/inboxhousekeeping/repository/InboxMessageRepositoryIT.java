package org.sunrider.inboxhousekeeping.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.sunrider.inboxhousekeeping.AbstractIntegrationTest;

class InboxMessageRepositoryIT extends AbstractIntegrationTest {

    @Autowired
    private InboxMessageRepository repository;

    @Nested
    @DisplayName("getOldPartitions")
    class GetOldPartitions {

        @Test
        @DisplayName("возвращает только партиции старше retention, в порядке по имени")
        void returnsOnlyExpiredOrderedByName() {
            String old30 = createPartition(today().minusDays(30));
            String old10 = createPartition(today().minusDays(10));
            String fresh = createPartition(today().minusDays(1));
            String todayPartition = createPartition(today());

            List<String> result = repository.getOldPartitions(7);

            assertThat(result).containsExactly(old30, old10);
            assertThat(result).doesNotContain(fresh, todayPartition, "inbox_message_default");
        }

        @Test
        @DisplayName("граница: партиция за today-8 попадает, за today-7 — нет (retention=7)")
        void boundaryIsInclusiveOnVerifiedDay() {
            String included = createPartition(today().minusDays(8));
            String excluded = createPartition(today().minusDays(7));

            List<String> result = repository.getOldPartitions(7);

            assertThat(result).contains(included);
            assertThat(result).doesNotContain(excluded);
        }

        @Test
        @DisplayName("default-партиция никогда не попадает в выборку")
        void excludesDefaultPartition() {
            // никаких дневных партиций нет — остаётся только inbox_message_default
            List<String> result = repository.getOldPartitions(0);

            assertThat(result).doesNotContain("inbox_message_default");
        }
    }

    @Nested
    @DisplayName("archiveErrorMessages")
    class ArchiveErrorMessages {

        @Test
        @DisplayName("архивирует только ERROR-записи, DONE игнорирует")
        void copiesOnlyErrors() {
            LocalDate day = today().minusDays(10);
            String partition = createPartition(day);
            insertMessages(day, "ERROR", 3);
            insertMessages(day, "DONE", 5);

            int processed = repository.archiveErrorMessages(partition, 1000);

            assertThat(processed).isEqualTo(3);
            assertThat(countRows("archive_inbox_message")).isEqualTo(3);
            Integer nonError = jdbc.queryForObject(
                "SELECT count(*) FROM archive_inbox_message WHERE status <> 'ERROR'", Integer.class);
            assertThat(nonError).isZero();
        }

        @Test
        @DisplayName("обрабатывает объём больше batchSize (батчинг по keyset)")
        void processesMoreThanOneBatch() {
            LocalDate day = today().minusDays(10);
            String partition = createPartition(day);
            insertMessages(day, "ERROR", 25);

            int processed = repository.archiveErrorMessages(partition, 10);

            assertThat(processed).isEqualTo(25);
            assertThat(countRows("archive_inbox_message")).isEqualTo(25);
        }

        @Test
        @DisplayName("идемпотентен: повторный прогон не плодит дубли в архиве")
        void isIdempotent() {
            LocalDate day = today().minusDays(10);
            String partition = createPartition(day);
            insertMessages(day, "ERROR", 5);

            repository.archiveErrorMessages(partition, 1000);
            repository.archiveErrorMessages(partition, 1000);

            assertThat(countRows("archive_inbox_message")).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("countUnarchivedErrors")
    class CountUnarchivedErrors {

        @Test
        @DisplayName("считает ERROR-записи, которых ещё нет в архиве, и 0 после архивации")
        void countsRemainingThenZero() {
            LocalDate day = today().minusDays(10);
            String partition = createPartition(day);
            insertMessages(day, "ERROR", 4);
            insertMessages(day, "DONE", 3);

            assertThat(repository.countUnarchivedErrors(partition)).isEqualTo(4);

            repository.archiveErrorMessages(partition, 1000);

            assertThat(repository.countUnarchivedErrors(partition)).isZero();
        }
    }

    @Nested
    @DisplayName("dropPartition")
    class DropPartition {

        @Test
        @DisplayName("удаляет партицию")
        void removesPartition() {
            String partition = createPartition(today().minusDays(10));
            assertThat(partitionExists(partition)).isTrue();

            repository.dropPartition(partition);

            assertThat(partitionExists(partition)).isFalse();
        }
    }

    @Nested
    @DisplayName("validatePartitionName")
    class ValidatePartitionName {

        @Test
        @DisplayName("отклоняет имена, не подходящие под формат партиции (защита от SQL-инъекции)")
        void rejectsInvalidNames() {
            assertThatThrownBy(() -> repository.dropPartition("inbox_message; DROP TABLE archive_inbox_message"))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> repository.archiveErrorMessages("not_a_partition", 10))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> repository.countUnarchivedErrors("not_a_partition"))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
