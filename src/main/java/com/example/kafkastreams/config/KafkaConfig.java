package com.example.kafkastreams.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
    
    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }
}
