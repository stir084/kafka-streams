package com.example.kafkastreams.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class ApiService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaStreamsService kafkaStreamsService;
    private static final String TOPIC_NAME = "api-calls";
    private static final String USER_TOPIC_NAME = "user-api-calls";

    public void processApiCall() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        kafkaTemplate.send(TOPIC_NAME, timestamp);
    }

    public void processUserApiCall(String userId) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        kafkaTemplate.send(USER_TOPIC_NAME, userId, timestamp);
    }

    public Map<String, Long> getHourlyStats() {
        return kafkaStreamsService.getHourlyStats();
    }

    public Map<String, Long> getUniqueUserStats() {
        return kafkaStreamsService.getUniqueUserStats();
    }
}
