package dev.vality.ccreporter.integration.support;

import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

/**
 * Выносит из Kafka integration-тестов техническую обвязку вокруг producer и ожидания записей в базе.
 */
public final class KafkaIntegrationTestSupport {

    private static final ThriftSerializer<SinkEvent> THRIFT_SERIALIZER = new ThriftSerializer<>();

    private KafkaIntegrationTestSupport() {
    }

    public static void waitForAssignments(
            KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
            EmbeddedKafkaBroker embeddedKafkaBroker
    ) {
        for (MessageListenerContainer listenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(
                    listenerContainer,
                    embeddedKafkaBroker.getPartitionsPerTopic()
            );
        }
    }

    public static void sendBatch(
            EmbeddedKafkaBroker embeddedKafkaBroker,
            String topic,
            List<MachineEvent> machineEvents
    ) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
                producerProperties(embeddedKafkaBroker)
        )) {
            for (MachineEvent machineEvent : machineEvents) {
                var sinkEvent = new SinkEvent();
                sinkEvent.setEvent(machineEvent);
                var payload = THRIFT_SERIALIZER.serialize("", sinkEvent);
                producer.send(new ProducerRecord<>(topic, machineEvent.getSourceId(), payload)).get();
            }
            producer.flush();
        }
    }

    public static Map<String, Object> waitForRow(
            JdbcTemplate jdbcTemplate,
            Duration timeout,
            String sql,
            Predicate<Map<String, Object>> predicate,
            Object... args
    ) throws InterruptedException {
        var deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            var rows = jdbcTemplate.queryForList(sql, args);
            if (!rows.isEmpty() && predicate.test(rows.getFirst())) {
                return rows.getFirst();
            }
            LockSupport.parkNanos(Duration.ofMillis(200L).toNanos());
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return jdbcTemplate.queryForList(sql, args).stream()
                .filter(predicate)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Kafka listener did not reach expected row state within " + timeout));
    }

    private static Properties producerProperties(EmbeddedKafkaBroker embeddedKafkaBroker) {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        return properties;
    }
}
