package com.hassan.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Topics and the shared listener error handler for the consumer side.
 */
@Configuration
public class KafkaListenerConfig {

    public static final String WIKIMEDIA_TOPIC = "wikimedia_recentchange";
    public static final String DLT_TOPIC = WIKIMEDIA_TOPIC + ".DLT";

    @Bean
    public NewTopic wikimediaTopic() {
        return TopicBuilder.name(WIKIMEDIA_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic wikimediaDltTopic() {
        return TopicBuilder.name(DLT_TOPIC).partitions(3).replicas(1).build();
    }

    /**
     * attempt → 2s → attempt → 2s → attempt → DLT.
     *
     * <p>{@code FixedBackOff}'s second argument counts <em>retries</em>, not total attempts,
     * so {@code (2000L, 2L)} is 3 delivery attempts. The default resolver would name the topic
     * {@code wikimedia_recentchange-dlt}; the explicit resolver keeps the {@code .DLT} suffix.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<?, ?> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template, (record, exception) -> new TopicPartition(DLT_TOPIC, record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 2L));
    }
}
