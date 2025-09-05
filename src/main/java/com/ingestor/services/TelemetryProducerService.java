package com.ingestor.services;

import com.dronelogistics.ingestorservice.DroneTelemetryEvent;
import com.ingestor.dtos.DroneTelemetryDataDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static com.ingestor.config.KafkaTopicConfig.KAFKA_DRONE_DATA_DLQ_TOPIC;
import static com.ingestor.config.KafkaTopicConfig.KAFKA_DRONE_DATA_TOPIC;

@Service
public class TelemetryProducerService implements ITelemetryProducerService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducerService.class);
    private final KafkaTemplate<String, DroneTelemetryEvent> kafkaTemplate;
    private final KafkaRetryService kafkaRetryService;

    public TelemetryProducerService(KafkaTemplate<String, DroneTelemetryEvent> kafkaTemplate, KafkaRetryService kafkaRetryService) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaRetryService = kafkaRetryService;
    }

    @Override
    public void sendTelemetryEvent(DroneTelemetryDataDto droneData) {
        log.info("Publishing data for drone id: {}", droneData.droneId());
        DroneTelemetryEvent event = buildDroneTelemetryEvent(droneData);
        String key = droneData.droneId().toString();
        try {
            publishMessageToKafkaQueue(KAFKA_DRONE_DATA_TOPIC, key, event);
        } catch (Exception e) {
            // send to dead letter queue
            try {
                publishMessageToKafkaQueue(KAFKA_DRONE_DATA_DLQ_TOPIC, key, event);
            } catch (Exception e2) {
                // Save in REDIS and try to process later
            }
        }
    }

    private DroneTelemetryEvent buildDroneTelemetryEvent(DroneTelemetryDataDto droneData) {
        return DroneTelemetryEvent.newBuilder()
                .setDroneId(droneData.droneId().toString())
                .setTimestamp(droneData.timestamp())
                .setLatitude(droneData.latitude())
                .setLongitude(droneData.longitude())
                .setVelocity(droneData.velocity())
                .setBatteryLevel(droneData.batteryLevel())
                .setEngineTemperature(droneData.engineTemperature())
                .build();
    }

    private void publishMessageToKafkaQueue(String kafkaTopic, String key, DroneTelemetryEvent event) throws Exception {
        kafkaRetryService.executeResilientWithRetry(() -> {
            var future = kafkaTemplate.send(kafkaTopic, key, event);
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send message for drone {} due to: {}", key, ex.getMessage(), ex);
                } else {
                    log.info("Message for drone {} sent successfully to partition {} with offset {}", key,
                            result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                }
            });
            return null;
        });
    }
}
