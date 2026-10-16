package com.hassan.kafka;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

/**
 * Turns one SSE message into one Kafka record:
 * <pre>
 * Wikimedia SSE → parse → key = event.id → JSON value → topic (3 partitions)
 * </pre>
 */
@Slf4j
@RequiredArgsConstructor
public class WikimediaChangesHandler implements BackgroundEventHandler {

    private final KafkaTemplate<String, WikimediaEvent> kafkaTemplate;
    private final String topic;

    private final JsonMapper objectMapper = JsonMapper.builder().build();

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        WikimediaEvent wikimediaEvent;
        try {
            wikimediaEvent = objectMapper.readValue(messageEvent.getData(), WikimediaEvent.class);
        } catch (JacksonException e) {
            // never let one malformed payload kill the SSE connection
            log.warn("Dropping unparsable event: {}", e.getOriginalMessage());
            return;
        }

        var headers = new RecordHeaders()
                .add("content-type", "application/json".getBytes(StandardCharsets.UTF_8))
                .add("event-source", "wikimedia-recentchange".getBytes(StandardCharsets.UTF_8));

        // key → same event always lands on the same partition (per-id ordering)
        // timestamp → explicit record timestamp; null would let the broker stamp it
        var record = new ProducerRecord<>(
                topic,
                null,
                System.currentTimeMillis(),
                wikimediaEvent.id(),
                wikimediaEvent,
                headers);

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event {}", wikimediaEvent.id(), ex);
                return;
            }

            var metadata = result.getRecordMetadata();

            log.info(
                    "Published event: topic={}, partition={}, offset={}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset()
            );
        });
    }

    @Override
    public void onOpen() {
    }

    @Override
    public void onClosed() {
    }

    @Override
    public void onComment(String comment) {
    }

    @Override
    public void onError(Throwable t) {
        log.error("Wikimedia stream error: {}", t.getMessage(), t);
    }
}
