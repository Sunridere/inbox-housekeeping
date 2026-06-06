package org.sunrider.inboxhousekeeping.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.sunrider.inboxhousekeeping.enums.InboxStatus;
import org.sunrider.inboxhousekeeping.repository.InboxMessageRepository;

@Slf4j
@Service
@RequiredArgsConstructor
public class InboxStatusMetrics {

    private final InboxMessageRepository inboxMessageRepository;
    private final MeterRegistry meterRegistry;

    private final Map<String, AtomicLong> inboxStatusMetrics = new ConcurrentHashMap<>();

    @PostConstruct
    private void init() {
        for (InboxStatus inboxStatus : InboxStatus.values()) {
            AtomicLong holder = new AtomicLong(0);
            inboxStatusMetrics.put(inboxStatus.name(), holder);

            Gauge.builder("inbox.message.rows", holder, AtomicLong::get)
                .tag("status", inboxStatus.name())
                .register(meterRegistry);
        }
    }

    public void refresh(){
        Map<String, Long> result = inboxMessageRepository.countByStatusInboxMessage();

        for (Map.Entry<String, AtomicLong> entry : inboxStatusMetrics.entrySet()) {
            entry.getValue().set(0);
        }

        for (Map.Entry<String, Long> entry : result.entrySet()) {
            String key = entry.getKey();
            Long value = entry.getValue();
            AtomicLong counter = inboxStatusMetrics.get(key);
            if (counter == null) {
                log.warn("Incorrect inbox status: {}", key);
                continue;
            }
            counter.set(value);
        }

        log.info("Inbox status metrics refreshed: {}", inboxStatusMetrics);

    }

}
