package org.sunrider.inboxhousekeeping.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.sunrider.inboxhousekeeping.service.InboxCleanupService;

@Slf4j
@Component
@RequiredArgsConstructor
public class InboxCleanupJob {

    private final InboxCleanupService inboxCleanupService;

    @Scheduled(cron = "${housekeeping.cron:0 0 2 * * *}")
    @SchedulerLock(
        name = "inboxCleanupJob",
        lockAtMostFor = "${housekeeping.lock-at-most-for:30m}",
        lockAtLeastFor = "${housekeeping.lock-at-least-for:5m}"
    )
    public void run() {
        log.info("Inbox cleanup job started");
        inboxCleanupService.processCleanup();
        log.info("Inbox cleanup job finished");
    }
}
