package org.example.Service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.example.Model.CountPair;
import org.example.Model.HumanBotDTO;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class MetricQueryService {
    private final KafkaStreams kafkaStreams;

    public CountPair getHumanVsBot() {
        ReadOnlyKeyValueStore<String,Long> botStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        "bot-count-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );
        Long botCount = botStore.get("BOT");

        ReadOnlyKeyValueStore<String, Long> humanStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        "human-count-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );

        Long humanCount = humanStore.get("HUMAN");
        return new CountPair(humanCount, botCount);
    }

    public CountPair getMajorVsMinor() {
        ReadOnlyKeyValueStore<String,Long> minorStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        "minor-count-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );
        Long minorCount = minorStore.get("MINOR");
        ReadOnlyKeyValueStore<String, Long> majorStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        "major-count-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );
        Long majorCount = majorStore.get("MAJOR");
        return new CountPair(majorCount, minorCount);
    }

    public Map<String,Long> getWikiMetrics(){
        ReadOnlyKeyValueStore<String,Long> wikiStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        "wiki-count-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );
        Map<String,Long> result = new HashMap<>();
        try(KeyValueIterator<String, Long> wikiIter = wikiStore.all()) {
            while (wikiIter.hasNext()) {
                KeyValue<String, Long> kv = wikiIter.next();
                result.put(kv.key,kv.value);
            }
        }
        return result;
    }


}
