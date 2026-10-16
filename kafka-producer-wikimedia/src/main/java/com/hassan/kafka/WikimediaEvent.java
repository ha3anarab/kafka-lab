package com.hassan.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Wire contract for the Wikimedia {@code recentchange} stream, published as JSON.
 *
 * <p>ponytail: this record is duplicated in {@code kafka-consumer-database}
 * ({@code com.hassan.kafka.event.WikimediaEvent}) because the two apps are
 * deployed separately. Extract a shared {@code wikimedia-events} module when a
 * third service starts consuming this topic.
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
