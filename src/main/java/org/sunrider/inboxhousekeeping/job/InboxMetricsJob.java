package org.sunrider.inboxhousekeeping.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.sunrider.inboxhousekeeping.service.InboxStatusMetrics;

@Slf4j
@Component
@RequiredArgsConstructor
public class InboxMetricsJob {

    private final InboxStatusMetrics inboxStatusMetrics;

    @Scheduled(fixedDelay = 5000)
    public void run(){
        inboxStatusMetrics.refresh();
    }

}
