package com.hassan.kafka;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.HttpConnectStrategy;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class WikimediaChangesProducer {

    private final KafkaTemplate<String, WikimediaEvent> kafkaTemplate;

    public void sendMessage() throws InterruptedException {

        var eventHandler = new WikimediaChangesHandler(kafkaTemplate, WikimediaTopicConfig.WIKIMEDIA_TOPIC);

        HttpConnectStrategy connectStrategy = ConnectStrategy.http(
                        URI.create("https://stream.wikimedia.org/v2/stream/recentchange"))
                .headers(new Headers.Builder()
                        .add("User-Agent", "KafkaLearningApp/1.0 (ha3an.arab@gmail.com)")
                        .build());

        try (BackgroundEventSource eventSource = new BackgroundEventSource.Builder(
                eventHandler,
                new EventSource.Builder(connectStrategy)
        ).build()) {
            eventSource.start();
            TimeUnit.MINUTES.sleep(10);
        }
    }
}
