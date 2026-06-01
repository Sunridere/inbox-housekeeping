package org.sunrider.inboxhousekeeping.job;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
    private final MeterRegistry meterRegistry;

    @Scheduled(cron = "${housekeeping.cron:0 0 2 * * *}")
    @SchedulerLock(
        name = "inboxCleanupJob",
        lockAtMostFor = "${housekeeping.lock-at-most-for:30m}",
        lockAtLeastFor = "${housekeeping.lock-at-least-for:5m}"
    )
    public void run() {
        Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Inbox cleanup job started");
        try {
            inboxCleanupService.processCleanup();
        } finally {
            long durationNanos = sample.stop(meterRegistry.timer("housekeeping.cleanup.duration"));
            log.info("Inbox cleanup job finished in {} ms", durationNanos / 1_000_000);
        }
    }
}
