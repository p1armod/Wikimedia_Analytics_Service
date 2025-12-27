package org.example.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

public class KafkaStreamsRunner {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-analytics-manual");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "Server_Address:9092"); //INSERT YOUR OWN ADDRESS(I WILL NOT GIVE YOU MY IP UNTIL I REMEMBER😁)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        KStream<String, String> stream = builder.stream("wikimedia.raw");

        stream
                .peek((k, v) -> System.out.println("RAW=" + v))
                .mapValues(v -> {
                    try {
                        return mapper.readTree(v);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter((k, v) -> v != null)
                .filter((k, v) -> "edit".equals(v.get("type").asText()))
                .selectKey((k, v) -> v.get("user").asText())
                .mapValues(JsonNode::toString) // 🔥 CRITICAL FIX (CONVERT BACK TO STRING)
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count()
                .toStream()
                .foreach((k, v) ->
                        System.out.println(
                                "User=" + k.key() +
                                        " Count=" + v +
                                        " WindowStart=" + k.window().start()
                        )
                );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }
}