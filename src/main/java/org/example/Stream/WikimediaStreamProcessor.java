package org.example.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.example.Model.*;
import org.example.Service.MetricQueryService;
import org.example.Service.RealtimeUpdatesService;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;

@Component
@RequiredArgsConstructor
@Slf4j
@EnableKafkaStreams
public class WikimediaStreamProcessor {

    private final StreamsBuilder builder;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RealtimeUpdatesService realtimeUpdatesService;
    private final MetricQueryService metricQueryService;

    Serde<DelayMetricDTO> delayMetricSerde =
            new JsonSerde<>(DelayMetricDTO.class);

    @PostConstruct
    public void buildTopology() {

        KStream<String, String> stream = builder.stream("wikimedia.raw");

        KStream<String, WikimediaEvent> events = stream
                .mapValues(v -> {
                    try {

                        return objectMapper.readValue(v,WikimediaEvent.class);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter((k, v) -> v != null);
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
        buildArrivalDelay(events);
        buildEditType(events);
    }

    private void buildArrivalDelay(KStream<String, WikimediaEvent> events) {

        events
                .groupBy(
                        (k, v) -> "DELAY",
                        Grouped.with(Serdes.String(), null)
                )

                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
                                .advanceBy(Duration.ofSeconds(5))
                )

                .aggregate(
                        DelayMetricDTO::new,

                        (key, event, agg) -> {
                            long delay = System.currentTimeMillis() - event.eventTime;

                            agg.count++;
                            agg.sum += delay;
                            agg.maxDelay = Math.max(agg.maxDelay, delay);

                            return agg;
                        },

                        Materialized.<String, DelayMetricDTO, WindowStore<Bytes, byte[]>>
                                        as("delay-metric-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(delayMetricSerde)
                );
    }

    private void buildEditType(KStream<String, WikimediaEvent> events) {
        events
                .filter((k,v) -> "edit".equals(v.type))
                .groupBy((k,v) -> v.type)
                .count(Materialized.as("edit-count-store"));

        events
                .filter((k,v) -> "new".equals(v.type))
                .groupBy((k,v) -> v.type)
                .count(Materialized.as("new-count-store"));

        events
                .filter((k,v) -> "log".equals(v.type))
                .groupBy((k,v) -> v.type)
                .count(Materialized.as("log-count-store"));
    }

    private void buildEditSize(KStream<String, WikimediaEvent> events) {
        events
                .filter((k,v) -> v.editSize >0 && v.editSize < 100)
                .groupBy((k,v)->"SMALL")
                .count(Materialized.as("small-count-store"));

        events
                .filter((k,v) -> v.editSize > 100 && v.editSize < 500)
                .groupBy((k,v)->"MEDIUM")
                .count(Materialized.as("medium-count-store"));

        events
                .filter((k,v) -> v.editSize > 500)
                .groupBy((k,v)->"LARGE")
                .count(Materialized.as("large-count-store"));
    }

    private void buildMajorVsMinor(KStream<String, WikimediaEvent> events) {
        events
                .filter((k,v)-> !v.minor)
                .groupBy((k,v)-> "MAJOR")
                .count(Materialized.as("major-count-store"));

        events
                .filter((k,v)-> v.minor)
                .groupBy((k,v)-> "MINOR")
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

    @Scheduled(fixedDelay = 1000)
    private void scheduled() {
        DashboardDTO dto = new DashboardDTO(
                metricQueryService.getHumanVsBot(),
                metricQueryService.getMajorVsMinor(),
                new HashMap<>(),
                new HashMap<>(),
                metricQueryService.getWikiMetrics(),
                new DelayMetricDTO()
        );
        DashboardMessageDTO<DashboardDTO> dashboardMessageDTO = new DashboardMessageDTO<DashboardDTO>("All Metrics", dto);
        realtimeUpdatesService.publish(dashboardMessageDTO);
    }
}