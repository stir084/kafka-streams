package com.example.kafkastreams.service;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Service
public class KafkaStreamsService {

    private static final String TOPIC_NAME = "api-calls";
    private static final String USER_TOPIC_NAME = "user-api-calls";
    private static final String STORE_NAME = "hourly-counts";
    private static final String USER_STORE_NAME = "user-counts";
    private KafkaStreams kafkaStreams;

    @Autowired
    private StreamsBuilder streamsBuilder;

    @PostConstruct
    public void init() {
        // 기존 시간별 API 호출 집계 스트림
        KStream<String, String> stream = streamsBuilder.stream(TOPIC_NAME, 
            Consumed.with(Serdes.String(), Serdes.String()));

        stream
            .groupBy((key, value) -> {
                LocalDateTime dateTime = LocalDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME);
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH"));
            }, Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as(STORE_NAME));

        // 새로운 사용자별 5분 윈도우 집계 스트림
        KStream<String, String> userStream = streamsBuilder.stream(USER_TOPIC_NAME,
            Consumed.with(Serdes.String(), Serdes.String()));

        // 사용자 ID로 그룹화하여 중복 제거 (KTable 사용)
        KTable<String, String> userTable = userStream
            .groupByKey()
            .reduce((oldValue, newValue) -> newValue);

        // 5분 윈도우로 집계
        userTable.toStream()
            .groupBy((userId, timestamp) -> "total")
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(USER_STORE_NAME)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "api-stats-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();
    }

    public Map<String, Long> getHourlyStats() {
        Map<String, Long> hourlyStats = new HashMap<>();
        ReadOnlyKeyValueStore<String, Long> keyValueStore =
            kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                STORE_NAME, 
                QueryableStoreTypes.<String, Long>keyValueStore()
            ));
        
        keyValueStore.all().forEachRemaining(entry -> 
            hourlyStats.put(entry.key, entry.value));
        
        return hourlyStats;
    }

    public Map<String, Long> getUniqueUserStats() {
        Map<String, Long> windowedStats = new HashMap<>();
        ReadOnlyWindowStore<String, Long> windowStore =
            kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                USER_STORE_NAME,
                QueryableStoreTypes.<String, Long>windowStore()
            ));

        Instant now = Instant.now();
        Instant from = now.minus(Duration.ofMinutes(30));

        try (WindowStoreIterator<Long> iterator = windowStore.fetch("total", from, now)) {
            while (iterator.hasNext()) {
                KeyValue<Long, Long> next = iterator.next();
                String windowStart = LocalDateTime
                    .ofInstant(Instant.ofEpochMilli(next.key), ZoneId.systemDefault())
                    .format(DateTimeFormatter.ISO_DATE_TIME);
                windowedStats.put(windowStart, next.value);
            }
        }

        return windowedStats;
    }
}
