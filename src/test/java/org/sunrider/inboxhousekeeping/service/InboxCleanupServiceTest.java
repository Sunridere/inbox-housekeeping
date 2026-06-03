package org.sunrider.inboxhousekeeping.service;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunrider.inboxhousekeeping.repository.InboxMessageRepository;

@ExtendWith(MockitoExtension.class)
class InboxCleanupServiceTest {

    private static final int RETENTION_DAYS = 7;
    private static final int BATCH_SIZE = 5000;

    @Mock
    private InboxMessageRepository repository;

    @InjectMocks
    private InboxCleanupService service;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "retentionDays", RETENTION_DAYS);
        ReflectionTestUtils.setField(service, "archiveBatchSize", BATCH_SIZE);
    }

    @Test
    @DisplayName("дропает партицию, когда все ERROR заархивированы")
    void dropsPartitionWhenFullyArchived() {
        String partition = "inbox_message_2020_01_01";
        when(repository.getOldPartitions(RETENTION_DAYS)).thenReturn(List.of(partition));
        when(repository.archiveErrorMessages(partition, BATCH_SIZE)).thenReturn(3);
        when(repository.countUnarchivedErrors(partition)).thenReturn(0);

        service.processCleanup();

        verify(repository).dropPartition(partition);
    }

    @Test
    @DisplayName("НЕ дропает партицию, если остались неархивированные ERROR (guard)")
    void skipsDropWhenUnarchivedErrorsRemain() {
        String partition = "inbox_message_2020_01_01";
        when(repository.getOldPartitions(RETENTION_DAYS)).thenReturn(List.of(partition));
        when(repository.archiveErrorMessages(partition, BATCH_SIZE)).thenReturn(0);
        when(repository.countUnarchivedErrors(partition)).thenReturn(5);

        service.processCleanup();

        verify(repository, never()).dropPartition(partition);
    }

    @Test
    @DisplayName("обрабатывает все старые партиции за один прогон")
    void processesAllExpiredPartitions() {
        String p1 = "inbox_message_2020_01_01";
        String p2 = "inbox_message_2020_01_02";
        when(repository.getOldPartitions(RETENTION_DAYS)).thenReturn(List.of(p1, p2));
        when(repository.archiveErrorMessages(p1, BATCH_SIZE)).thenReturn(1);
        when(repository.archiveErrorMessages(p2, BATCH_SIZE)).thenReturn(2);
        when(repository.countUnarchivedErrors(p1)).thenReturn(0);
        when(repository.countUnarchivedErrors(p2)).thenReturn(0);

        service.processCleanup();

        verify(repository).dropPartition(p1);
        verify(repository).dropPartition(p2);
    }
}
