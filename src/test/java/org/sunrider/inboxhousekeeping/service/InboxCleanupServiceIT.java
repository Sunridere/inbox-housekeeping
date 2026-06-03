package org.sunrider.inboxhousekeeping.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.sunrider.inboxhousekeeping.AbstractIntegrationTest;

/**
 * E2E-проверка полного цикла очистки на реальном PostgreSQL.
 * retention в профиле test = 7 дней (партиция дропается, если её дата <= today-8).
 */
class InboxCleanupServiceIT extends AbstractIntegrationTest {

    @Autowired
    private InboxCleanupService service;

    @Test
    @DisplayName("архивирует ERROR старой партиции и дропает её, свежую не трогает")
    void archivesErrorsAndDropsOldPartition() {
        LocalDate oldDay = today().minusDays(10);
        String oldPartition = createPartition(oldDay);
        insertMessages(oldDay, "ERROR", 4);
        insertMessages(oldDay, "DONE", 6);

        LocalDate freshDay = today().minusDays(1);
        String freshPartition = createPartition(freshDay);
        insertMessages(freshDay, "ERROR", 3);

        service.processCleanup();

        // старая партиция: ERROR в архиве, сама партиция удалена
        assertThat(countRows("archive_inbox_message")).isEqualTo(4);
        assertThat(partitionExists(oldPartition)).isFalse();

        // свежая партиция нетронута
        assertThat(partitionExists(freshPartition)).isTrue();
        assertThat(countRows(freshPartition)).isEqualTo(3);
    }

    @Test
    @DisplayName("если старых партиций нет — ничего не архивирует и не дропает")
    void noopWhenNothingExpired() {
        LocalDate freshDay = today().minusDays(1);
        String freshPartition = createPartition(freshDay);
        insertMessages(freshDay, "ERROR", 5);

        service.processCleanup();

        assertThat(countRows("archive_inbox_message")).isZero();
        assertThat(partitionExists(freshPartition)).isTrue();
        assertThat(countRows(freshPartition)).isEqualTo(5);
    }
}
