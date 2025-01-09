package com.example.kafkastreams.controller;

import com.example.kafkastreams.service.ApiService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ApiController {

    private final ApiService apiService;

    @PostMapping("/a")
    public ResponseEntity<String> callApiA() {
        apiService.processApiCall();
        return ResponseEntity.ok("API A called successfully");
    }

    @GetMapping("/b")
    public ResponseEntity<Map<String, Long>> getHourlyStats() {
        return ResponseEntity.ok(apiService.getHourlyStats());
    }

    @PostMapping("/c/{userId}")
    public ResponseEntity<String> callApiC(@PathVariable String userId) {
        apiService.processUserApiCall(userId);
        return ResponseEntity.ok("API C called successfully");
    }

    @GetMapping("/d")
    public ResponseEntity<Map<String, Long>> getUniqueUserStats() {
        return ResponseEntity.ok(apiService.getUniqueUserStats());
    }
}
