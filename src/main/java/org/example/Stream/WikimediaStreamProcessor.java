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
import org.example.Model.WikimediaEvent;
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

        KStream<String, WikimediaEvent> events = stream
                .mapValues(v -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        return mapper.readValue(v,WikimediaEvent.class);
                    } catch (Exception e) {
                        return null;
                    }
                });
//                .filter((k, v) -> v != null)
//                .filter((k, v) -> "edit".equals(v.get("type").asText()))
//                .selectKey((k, v) -> v.get("user").asText())
//                .mapValues(JsonNode::toString) // 🔥 critical fix
//                .groupByKey()
//                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//                .count(Materialized.as("edit-count-by-user"))
//                .toStream()
//                .foreach((k, v) ->{
//                    EditCountEvent editEvent = new EditCountEvent(k.key(),v,k.window().start());
//                    realtimeUpdatesService.publish(editEvent);
//                });

        buildBotVsHuman(events);
        buildWikiMetric(events);
        buildMajorVsMinor(events);
        buildEditSize(events);
//        buildEditOverTime(events);
//        buildArrivalDelay(events);
    }

    private void buildEditSize(KStream<String, WikimediaEvent> events) {
        events
                .filter((k,v) -> v.editSize >0 && v.editSize < 100)
                .groupBy((k,v)->v.wiki)
                .count(Materialized.as("small-count-store"));

        events
                .filter((k,v) -> v.editSize > 100 && v.editSize < 500)
                .groupBy((k,v)->v.wiki)
                .count(Materialized.as("medium-count-store"));

        events
                .filter((k,v) -> v.editSize > 500)
                .groupBy((k,v)->v.wiki)
                .count(Materialized.as("large-count-store"));
    }

    private void buildMajorVsMinor(KStream<String, WikimediaEvent> events) {
        events
                .filter((k,v)-> v.editSize > 100)
                .groupBy((k,v)-> v.wiki)
                .count(Materialized.as("major-count-store"));

        events
                .filter((k,v)-> v.editSize < 100)
                .groupBy((k,v)-> v.wiki)
                .count(Materialized.as("minor-count-store"));
    }

    private void buildWikiMetric(KStream<String, WikimediaEvent> events) {
        events
                .groupBy((k, v) -> v.wiki)
                .count(Materialized.as("wiki-count-store"));
    }

    private void buildBotVsHuman(KStream<String, WikimediaEvent> events) {
        events
                .filter((k,v) -> v.bot)
                .groupBy((k,v)-> "BOT")
                .count(Materialized.as("bot-count-store"));

        events
                .filter((k,v) -> !v.bot)
                .groupBy((k,v)-> "HUMAN")
                .count(Materialized.as("human-count-store"));
    }
}