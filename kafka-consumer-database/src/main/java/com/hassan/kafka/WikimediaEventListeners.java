package com.hassan.kafka;

import com.hassan.kafka.entity.WikimediaEventEntity;
import com.hassan.kafka.event.WikimediaEvent;
import com.hassan.kafka.repository.WikimediaDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static com.hassan.kafka.KafkaListenerConfig.DLT_TOPIC;
import static com.hassan.kafka.KafkaListenerConfig.WIKIMEDIA_TOPIC;

/**
 * Four independent consumer groups reading the same topic:
 * <pre>
 * wikimedia_recentchange ─┬─ myGroup     → database (idempotent)
 *                         ├─ analytics   → statistics
 *                         └─ monitoring  → partition/offset distribution
 * wikimedia_recentchange.DLT ─ dlt-monitor → dead letters after retries
 * </pre>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WikimediaEventListeners {

    private final WikimediaDataRepository repository;

    private final LongAdder persisted = new LongAdder();
    private final Map<String, LongAdder> eventsByType = new ConcurrentHashMap<>();
    private final Map<Integer, LongAdder> eventsByPartition = new ConcurrentHashMap<>();

    @KafkaListener(topics = WIKIMEDIA_TOPIC, groupId = "myGroup")
    public void consume(
            WikimediaEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info(
                "partition={}, offset={}, event={}",
                partition,
                offset,
                event.id()
        );
        log.info("Event: id={}, type={}, title={}", event.id(), event.type(), event.title());

        // step 5 — a poison message that always fails, so retries → DLT can be observed
        if (event.title() != null && event.title().contains("FAIL")) {
            throw new IllegalStateException("Simulated failure for event " + event.id());
        }

        // step 7 — at-least-once delivery means redelivery happens; the UNIQUE column on
        // eventId is the real guard, existsByEventId() just avoids a pointless insert attempt.
        if (repository.existsByEventId(event.id())) {
            log.debug("Duplicate {} ignored", event.id());
            return;
        }

        repository.save(toEntity(event));
        persisted.increment();
        log.info("Stored event {} (total {})", event.id(), persisted.sum());
    }

    /** step 3 — independent group: sees every message regardless of myGroup's progress. */
    @KafkaListener(topics = WIKIMEDIA_TOPIC, groupId = "analytics")
    public void analytics(WikimediaEvent event) {
        eventsByType.computeIfAbsent(event.type(), type -> new LongAdder()).increment();
        long total = eventsByType.values().stream().mapToLong(LongAdder::sum).sum();
        if (total % 50 == 0) {
            log.info("analytics: total={}, byType={}", total, eventsByType);
        }
    }

    /** step 4 — same topic, third group: shows keyed events spread across the 3 partitions. */
    @KafkaListener(topics = WIKIMEDIA_TOPIC, groupId = "monitoring")
    public void monitoring(
            WikimediaEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {

        var counter = eventsByPartition.computeIfAbsent(partition, p -> new LongAdder());
        counter.increment();
        long onPartition = counter.sum();
        long sum = eventsByPartition.values().stream().mapToLong(LongAdder::sum).sum();
        if (sum % 50 == 0) {
            log.info("monitoring: total={}, byPartition={} (partition {} handled {})",
                    sum, eventsByPartition, partition, onPartition);
        }
    }

    /** step 6 — where a message ends up after the error handler gave up. */
    @KafkaListener(topics = DLT_TOPIC, groupId = "dlt-monitor")
    public void deadLetter(
            WikimediaEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.warn("Dead letter: partition={}, offset={}, event={}, title={}",
                partition, offset, event.id(), event.title());
    }

    private static WikimediaEventEntity toEntity(WikimediaEvent event) {
        var row = new WikimediaEventEntity();
        row.setEventId(event.id());
        row.setType(event.type());
        row.setTitle(event.title());
        row.setUser(event.user());
        row.setServerName(event.serverName());
        row.setTimestamp(event.timestamp());
        return row;
    }
}
