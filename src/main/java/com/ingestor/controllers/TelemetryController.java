package com.ingestor.controllers;


import com.ingestor.dtos.DroneTelemetryDataDto;
import com.ingestor.dtos.ErrorResponseDto;
import com.ingestor.services.ITelemetryProducerService;
import com.ingestor.services.TelemetryProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/drone/telemetry")
public class TelemetryController {

    private final ITelemetryProducerService telemetryProducerService;

    public TelemetryController(TelemetryProducerService telemetryProducerService) {
        this.telemetryProducerService = telemetryProducerService;
    }

    @PostMapping
    public ResponseEntity<?> publishDroneTelemetryData(@RequestBody DroneTelemetryDataDto droneTelemetryDataDto) {
        try {
            telemetryProducerService.sendTelemetryEvent(droneTelemetryDataDto);
            return ResponseEntity.accepted().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(new ErrorResponseDto(e.getMessage()));
        }
    }
}
