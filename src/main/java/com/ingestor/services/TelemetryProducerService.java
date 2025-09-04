package com.ingestor.services;

import com.dronelogistics.ingestorservice.DroneTelemetryEvent;
import com.ingestor.dtos.DroneTelemetryDataDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static com.ingestor.config.KafkaTopicConfig.KAFKA_DRONE_DATA_TOPIC;

@Service
public class TelemetryProducerService implements ITelemetryProducerService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducerService.class);
    private final KafkaTemplate<String, DroneTelemetryEvent> kafkaTemplate;

    public TelemetryProducerService(KafkaTemplate<String, DroneTelemetryEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void sendTelemetryEvent(DroneTelemetryDataDto droneData) throws Exception {
        try {
            log.info("Publishing data for drone id: {}", droneData.droneId());
            DroneTelemetryEvent event = DroneTelemetryEvent.newBuilder()
                    .setDroneId(droneData.droneId().toString())
                    .setTimestamp(droneData.timestamp())
                    .setLatitude(droneData.latitude())
                    .setLongitude(droneData.longitude())
                    .setVelocity(droneData.velocity())
                    .setBatteryLevel(droneData.batteryLevel())
                    .setEngineTemperature(droneData.engineTemperature())
                    .build();
            kafkaTemplate.send(KAFKA_DRONE_DATA_TOPIC, event);
        } catch (Exception e) {
            log.error("Error to publish telemetry for drone {} due to: {}", droneData.droneId(), e.getMessage(), e);
            throw e;
        }
    }
}
