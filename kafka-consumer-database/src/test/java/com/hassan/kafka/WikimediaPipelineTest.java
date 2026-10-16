package com.hassan.kafka;

import com.hassan.kafka.event.WikimediaEvent;
import com.hassan.kafka.repository.WikimediaDataRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end check of the pipeline against an in-JVM broker:
 * JSON publish → 3 consumer groups → idempotent DB write, and retry → DLT for poison messages.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:kafkalab;DB_CLOSE_DELAY=-1",
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
@EmbeddedKafka(partitions = 3,
        topics = {KafkaListenerConfig.WIKIMEDIA_TOPIC, KafkaListenerConfig.DLT_TOPIC})
class WikimediaPipelineTest {

    @Autowired
    private KafkaTemplate<String, WikimediaEvent> kafkaTemplate;

    @Autowired
    private WikimediaDataRepository repository;

    @Value("${spring.kafka.bootstrap-servers}")
    private String brokers;

    @Test
    void redeliveredEventsAreStoredOnce() throws Exception {
        var event = new WikimediaEvent(
                "evt-1", "edit", "Page", "alice", "en.wikipedia.org", "2026-10-03T10:00:00Z",
                null);

        publish(event, event.id());
        await(() -> repository.countByEventId("evt-1") == 1);

        // at-least-once: after a rebalance or a crash between "DB committed" and
        // "offset committed" the same record comes back. Same key → same partition → FIFO,
        // so a barrier published under the same key is only stored once every duplicate
        // above it has been processed — no sleeps, no timing assumptions.
        for (int i = 0; i < 3; i++) {
            publish(event, event.id());
        }
        publish(new WikimediaEvent(
                        "evt-1-barrier", "edit", "Barrier", "alice", "en.wikipedia.org",
                        "2026-10-03T10:00:01Z", null),
                event.id());

        await(() -> repository.countByEventId("evt-1-barrier") == 1);
        assertEquals(1, repository.countByEventId("evt-1"),
                "the same event id must be stored exactly once");
    }

    @Test
    void poisonMessageIsRetriedThenDeadLettered() throws Exception {
        publish(new WikimediaEvent(
                "evt-fail", "edit", "FAIL page", "alice", "en.wikipedia.org", "2026-10-03T10:00:02Z",
                null),
                "evt-fail");

        try (var consumer = dltConsumer()) {
            var dead = awaitRecord(consumer,
                    record -> record.value() != null && "evt-fail".equals(record.value().id()));

            assertNotNull(dead, "after the retries the record must be in " + KafkaListenerConfig.DLT_TOPIC);
            assertEquals(KafkaListenerConfig.DLT_TOPIC, dead.topic());
        }

        assertEquals(0, repository.countByEventId("evt-fail"),
                "a message that always fails must never reach the database");
    }

    private void publish(WikimediaEvent event, String key) throws Exception {
        kafkaTemplate.send(KafkaListenerConfig.WIKIMEDIA_TOPIC, key, event)
                .get(10, TimeUnit.SECONDS);
    }

    /** A fresh group with {@code earliest} reads the DLT from its beginning. */
    private KafkaConsumer<String, WikimediaEvent> dltConsumer() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlt-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);
        props.put("spring.json.value.default.type", WikimediaEvent.class.getName());
        props.put("spring.json.use.type.headers", "false");

        var consumer = new KafkaConsumer<String, WikimediaEvent>(props);
        consumer.subscribe(List.of(KafkaListenerConfig.DLT_TOPIC));
        return consumer;
    }

    private static <T> ConsumerRecord<String, T> awaitRecord(
            KafkaConsumer<String, T> consumer, Predicate<ConsumerRecord<String, T>> match) {

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            for (var record : consumer.poll(Duration.ofSeconds(1))) {
                if (match.test(record)) {
                    return record;
                }
            }
        }
        return null;
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition not met within 60s");
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
            }
        }
    }
}
