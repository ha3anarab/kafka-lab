package com.hassan.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics owned by the producer side. The dead-letter topic is declared by the
 * consumer module, next to the error handler that writes to it.
 */
@Configuration
public class WikimediaTopicConfig {

    public static final String WIKIMEDIA_TOPIC = "wikimedia_recentchange";

    @Bean
    public NewTopic wikimediaTopic() {
        return TopicBuilder.name(WIKIMEDIA_TOPIC).partitions(3).replicas(1).build();
    }
}
