package org.sunrider.inboxhousekeeping.service;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.sunrider.inboxhousekeeping.repository.InboxMessageRepository;

@Slf4j
@Service
@RequiredArgsConstructor
public class InboxCleanupService {

    private final InboxMessageRepository inboxMessageRepository;

    @Value("${housekeeping.error-retention-days:30}")
    private int retentionDays;

    @Value("${housekeeping.archive-batch-size:5000}")
    private int archiveBatchSize;

    /**
     * Намеренно не @Transactional: каждый батч архивации и DROP должны
     * коммититься отдельно, чтобы не держать долгую транзакцию и дробить WAL.
     */
    public void processCleanup() {
        List<String> expiredPartition = inboxMessageRepository.getOldPartitions(retentionDays);
        int totalArchived = 0;
        int partitionsDeleted = 0;
        for (String partitionName : expiredPartition) {
            try {
                totalArchived += inboxMessageRepository.archiveErrorMessages(partitionName,
                    archiveBatchSize);

                // Защита от потери данных: не дропаем, пока в архиве нет всех ERROR-записей.
                int unarchived = inboxMessageRepository.countUnarchivedErrors(partitionName);
                if (unarchived > 0) {
                    log.error("Skip dropping partition {}: {} ERROR rows are not archived yet",
                        partitionName, unarchived);
                    continue;
                }
                inboxMessageRepository.dropPartition(partitionName);
                partitionsDeleted++;
            }catch (Exception e) {
                log.error("Error dropping partition {}", partitionName, e);
            }
        }
        log.info("Total archived error messages: {} from {} partitions",
            totalArchived, partitionsDeleted);
    }



}
