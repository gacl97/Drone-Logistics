package com.ingestor.services;

import com.ingestor.dtos.DroneTelemetryDataDto;

public interface ITelemetryProducerService {

    void sendTelemetryEvent(DroneTelemetryDataDto droneTelemetryDataDto);
}
