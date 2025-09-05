package com.ingestor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    public static final String KAFKA_DRONE_DATA_TOPIC = "drone-telemetry-data";
    public static final String KAFKA_DRONE_DATA_DLQ_TOPIC = "drone-telemetry-data";

    @Bean
    public NewTopic droneTelemetryDataTopic() {
        return new NewTopic(KAFKA_DRONE_DATA_TOPIC, 2, (short) 1);
    }

    @Bean
    public NewTopic droneTelemetryDataDeadLetterQueueTopic() {
        return new NewTopic(KAFKA_DRONE_DATA_DLQ_TOPIC, 2, (short) 1);
    }
}