package com.lokesh.streams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import com.lokesh.streams.KafkaStreamsApplication.LogEvent;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import io.confluent.common.utils.TestUtils;

public class KStreamDistinct {

    private static final String storeName = "eventId-store";

    private static class DeduplicationTransformer<K, V, E> implements ValueTransformerWithKey<K, V, V> {

        private ProcessorContext context;
        private WindowStore<E, Long> eventIdStore;

        private final long leftDurationMs;
        private final long rightDurationMs;

        private final KeyValueMapper<K, V, E> idExtractor;

        DeduplicationTransformer(final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
            if (maintainDurationPerEventInMs < 1) {
                throw new IllegalArgumentException("maintain duration per event must be >= 1");
            }
            leftDurationMs = maintainDurationPerEventInMs / 2;
            rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
            this.idExtractor = idExtractor;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            this.context = context;
            eventIdStore = (WindowStore<E, Long>) context.getStateStore(storeName);
        }

        @Override
        public V transform(final K key, final V value) {
            final E eventId = idExtractor.apply(key, value);
            if (eventId == null) {
                return value;
            } else {
                final V output;
                if (isDuplicate(eventId)) {
                    output = null;
                    updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
                } else {
                    output = value;
                    rememberNewEvent(eventId, context.timestamp());
                }
                return output;
            }
        }

        private boolean isDuplicate(final E eventId) {
            final long eventTime = context.timestamp();
            final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
                    eventId,
                    eventTime - leftDurationMs,
                    eventTime + rightDurationMs);
            final boolean isDuplicate = timeIterator.hasNext();
            timeIterator.close();
            return isDuplicate;
        }

        private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, final long newTimestamp) {
            eventIdStore.put(eventId, newTimestamp, newTimestamp);
        }

        private void rememberNewEvent(final E eventId, final long timestamp) {
            eventIdStore.put(eventId, timestamp, timestamp);
        }

        @Override
        public void close() {
            // Note: The store should NOT be closed manually here via
            // `eventIdStore.close()`!
            // The Kafka Streams API will automatically close stores when necessary.
        }

    }

    private Serde<LogEvent> buildLogSerde(final Properties allProps) {
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<LogEvent> logeventSerializer = new JsonSerializer<>();
        serdeProps.put("JsonPOJOClass", LogEvent.class);
        logeventSerializer.configure(serdeProps, false);

        final Deserializer<LogEvent> logeventDeSerializer = new JsonDeserializer<>();
        serdeProps.put("JsonPOJOClass", LogEvent.class);
        logeventDeSerializer.configure(serdeProps, false);

        return Serdes.serdeFrom(logeventSerializer, logeventDeSerializer);
    }

    @SuppressWarnings("deprecation")
    public Topology buildTopology(Properties allProps,
            final Serde<LogEvent> logSerde) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String inputTopic = allProps.getProperty("input.topic.name");
        final String outputTopic = allProps.getProperty("output.topic.name");
        final Duration windowSize = Duration.ofMinutes(10);
        final Duration retentionPeriod = windowSize;
        final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName,
                        retentionPeriod,
                        windowSize,
                        false),
                Serdes.String(),
                Serdes.Long());
        builder.addStateStore(dedupStoreBuilder);
        builder
                .stream(inputTopic, Consumed.with(Serdes.String(), logSerde))
                .transformValues(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
                        (key, logEvent) -> logEvent.exception != null ? logEvent.exception.exception_class : null),
                        storeName)
                .filter((k, v) -> v != null)
                .to(outputTopic, Produced.with(Serdes.String(), logSerde));
        return builder.build();
    }

    public void createTopics(Properties allProps) {
        AdminClient client = AdminClient.create(allProps);

        List<NewTopic> topics = new ArrayList<>();
        // topics.add(new NewTopic(
        // allProps.getProperty("input.topic.name"),
        // Integer.parseInt(allProps.getProperty("input.topic.partitions")),
        // Short.parseShort(allProps.getProperty("input.topic.replication.factor"))));
        topics.add(new NewTopic(
                allProps.getProperty("output.topic.name"),
                Integer.parseInt("1"),
                Short.parseShort("1")));

        client.createTopics(topics);
        client.close();
    }

    public static Properties loadEnvProperties(String fileName) throws IOException {
        Properties allProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        allProps.load(input);
        input.close();

        return allProps;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "This program takes one argument: the path to an environment configuration file.");
        }

        new KStreamDistinct().runRecipe(args[0]);
    }

    private void runRecipe(final String configPath) throws IOException {
        final Properties allProps = new Properties();
        try (InputStream inputStream = new FileInputStream(configPath)) {
            allProps.load(inputStream);
        }
        allProps.put(StreamsConfig.APPLICATION_ID_CONFIG, allProps.getProperty("application.id"));
        allProps.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

        final Topology topology = this.buildTopology(allProps, this.buildLogSerde(allProps));

        this.createTopics(allProps);

        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }

}
