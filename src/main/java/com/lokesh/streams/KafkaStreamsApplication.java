package com.lokesh.streams;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamsApplication {

    // POJO classes
    static public class LogEvent {
        public Exception exception;
        public int version;
        public String source_host;
        public String message;
        public String thread_name;
        public String timestamp;
        public String level;
        public String logger_name;

    }

    static public class Exception{
        public String exception_class;
        public String exception_message;
        public String stacktrace;
    }

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);
    

    @SuppressWarnings("unlikely-arg-type")
    public static void main(String[] args) {
        logger.info("Hello world!");


        // Any further settings
        final String inputTopic = "splunklogs";
        final String outputTopic = "splunklogs-transformed";


        Properties props = new Properties();
        // Set a few key parameters
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, logEventSerde);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");



        KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(inputTopic, outputTopic), props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });
        try {
            kafkaStreams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);

    }

    public static Topology buildTopology(String inputTopic, String outputTopic) {

        final StreamsBuilder builder = new StreamsBuilder();

        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<LogEvent> logeventSerializer = new JsonSerializer<>();
        serdeProps.put("JsonPOJOClass", LogEvent.class);
        logeventSerializer.configure(serdeProps, false);

        final Deserializer<LogEvent> logeventDeSerializer = new JsonDeserializer<>();
        serdeProps.put("JsonPOJOClass", LogEvent.class);
        logeventDeSerializer.configure(serdeProps, false);

        final Serde<LogEvent> logEventSerde = Serdes.serdeFrom(logeventSerializer, logeventDeSerializer);

        final KStream<String, LogEvent> logEventSteam = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), logEventSerde));
        logEventSteam.filter((k, logevent) -> logevent.level.equals("INFO")).to(outputTopic,
                Produced.with(Serdes.String(), logEventSerde));

        return builder.build();
    }

}