package com.lokesh.streams;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;

import com.lokesh.streams.KafkaStreamsApplication.LogEvent;

public class KafkaStreamsApplicationTest {

    private final static String TEST_CONFIG_FILE = "/Users/lallam/Downloads/Ameren_PS_Splunk/examples/src/main/resources/test.properties";

    @Test
    public void topologyShouldUpperCaseInputs() throws FileNotFoundException, IOException {

        final Properties props = new Properties();
        try (InputStream inputStream = new FileInputStream(TEST_CONFIG_FILE)) {
            props.load(inputStream);
        }

        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<LogEvent> logeventSerializer = new JsonSerializer<>();
        serdeProps.put("JsonPOJOClass", LogEvent.class);
        logeventSerializer.configure(serdeProps, false);

        final Deserializer<LogEvent> logeventDeSerializer = new JsonDeserializer<>();
        serdeProps.put("JsonPOJOClass", LogEvent.class);
        logeventDeSerializer.configure(serdeProps, false);

        final Serde<LogEvent> logEventSerde = Serdes.serdeFrom(logeventSerializer, logeventDeSerializer);

        final String inputTopicName = props.getProperty("input.topic.name");
        final String outputTopicName = props.getProperty("output.topic.name");

        final Topology topology = KafkaStreamsApplication.buildTopology(inputTopicName, outputTopicName);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, props)) {

            Serde<String> stringSerde = Serdes.String();

            final TestInputTopic<String, LogEvent> inputTopic = testDriver.createInputTopic(
                    inputTopicName,
                    stringSerde.serializer(), logEventSerde.serializer());

            final TestOutputTopic<String, LogEvent> outputTopic = testDriver.createOutputTopic(
                    outputTopicName,
                    stringSerde.deserializer(), logEventSerde.deserializer());

            String inputJson = "{\"version\":1,\"source_host\":\"DXQR1PVK6D\",\"message\":\"Some Failure\",\"thread_name\":\"main\",\"timestamp\":\"2024-02-05T14:59:01.614-0500\",\"level\":\"ERROR\",\"logger_name\":\"com.lokesh.poc.Log4jTestClass\"}";

            LogEvent logEvent = new LogEvent();
            logEvent.level = "INFO";
            logEvent.version = 1;
            logEvent.logger_name = "test";
            logEvent.source_host = "localhost";
            logEvent.thread_name = "testthread";
            logEvent.timestamp = "time";
            logEvent.message = "message";

            inputTopic.pipeInput(logEvent);

            System.out.println(outputTopic.readValue().toString());
        }

    }

}
