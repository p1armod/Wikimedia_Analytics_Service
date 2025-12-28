package org.example.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.example.Model.EditCountEvent;
import org.example.Service.RealtimeUpdatesService;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
@Slf4j
@EnableKafkaStreams
public class WikimediaStreamProcessor {

    private final StreamsBuilder builder;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RealtimeUpdatesService realtimeUpdatesService;

    @PostConstruct
    public void buildTopology() {

        KStream<String, String> stream = builder.stream("wikimedia.raw");

        stream
//                .peek((k, v) -> log.info("RAW={}", v))
                .mapValues(v -> {
                    try {
                        return objectMapper.readTree(v);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter((k, v) -> v != null)
                .filter((k, v) -> "edit".equals(v.get("type").asText()))
                .selectKey((k, v) -> v.get("user").asText())
                .mapValues(JsonNode::toString) // 🔥 critical fix
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.as("edit-count-by-user"))
                .toStream()
                .foreach((k, v) ->{
                    EditCountEvent editEvent = new EditCountEvent(k.key(),v,k.window().start());
                    realtimeUpdatesService.publish(editEvent);
                });
    }
}