package com.hassan.kafka.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Wire contract of {@code wikimedia_recentchange}, as produced by the producer module.
 *
 * <p>ponytail: duplicated from {@code com.hassan.kafka.WikimediaEvent} (producer) because the
 * two apps are deployed separately and no Schema Registry is available offline. Extract a
 * shared {@code wikimedia-events} module — or move to Avro + Schema Registry — when a third
 * service consumes this topic.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WikimediaEvent(
        String id,
        String type,
        String title,
        String user,
        @JsonProperty("server_name") String serverName,
        String timestamp,
        Meta meta
) {

    /**
     * {@code log} events carry no top-level {@code id} but every event has {@code meta.id}.
     * {@code meta} is input-only plumbing: resolved into {@code id}, then dropped so it never
     * reaches the wire.
     */
    public WikimediaEvent {
        if (id == null && meta != null) {
            id = meta.id();
        }
        meta = null;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Meta(String id) {
    }
}
