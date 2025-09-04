package com.ingestor.dtos;


import java.util.UUID;

public record DroneTelemetryDataDto(UUID droneId, Long timestamp, Double latitude, Double longitude, Double velocity,
                                    Double batteryLevel, Double engineTemperature) {
}
