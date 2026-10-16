package com.hassan.kafka;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The producer maps live Wikimedia SSE payloads onto {@link WikimediaEvent}:
 * unknown fields are ignored, snake_case is mapped, numeric scalars coerce to String,
 * and an absent {@code id} falls back to {@code meta.id} so the DB unique key is never null.
 */
class WikimediaEventTest {

    private final JsonMapper objectMapper = JsonMapper.builder().build();

    private static final String RECENTCHANGE = """
            {"id":3982847422,"type":"edit","namespace":0,"title":"Test page",
             "timestamp":1726456789.123,"user":"ExampleBot","bot":true,"minor":false,
             "length":{"old_len":100,"new_len":200},"comment":"typo fix","wiki":"enwiki",
             "server_name":"en.wikipedia.org","server_url":"https://en.wikipedia.org",
             "server_script_path":"/w","meta":{"id":"abc","domain":"en.wikipedia.org",
             "time":"2026-10-03T10:00:00Z","uri":"https://en.wikipedia.org/x"}}
            """;

    @Test
    void mapsRecentChangePayload() throws Exception {
        var event = objectMapper.readValue(RECENTCHANGE, WikimediaEvent.class);

        assertEquals("3982847422", event.id());
        assertEquals("edit", event.type());
        assertEquals("Test page", event.title());
        assertEquals("ExampleBot", event.user());
        assertEquals("en.wikipedia.org", event.serverName());
        assertNotNull(event.timestamp());
        assertEquals("1726456789.123", event.timestamp());
    }

    @Test
    void fallsBackToMetaIdWhenTopLevelIdIsAbsent() throws Exception {
        // real "log" events have no top-level id — but they do have meta.id
        var json = """
                {"type":"log","log_type":"rename","title":"Old name","timestamp":1726456790.0,
                 "user":"ExampleBot","server_name":"en.wikipedia.org",
                 "meta":{"id":"76f2291a-234e-4789-8782-b54cd44ca357","domain":"en.wikipedia.org"}}
                """;

        var event = objectMapper.readValue(json, WikimediaEvent.class);

        assertEquals("76f2291a-234e-4789-8782-b54cd44ca357", event.id());

        // meta is input-only plumbing: it must not be re-serialized into the Kafka value
        var serialized = objectMapper.writeValueAsString(event);
        assertFalse(serialized.contains("meta"), serialized);
        assertTrue(serialized.contains("76f2291a"), serialized);
    }
}
