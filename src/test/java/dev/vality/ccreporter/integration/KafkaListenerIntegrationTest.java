package dev.vality.ccreporter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@EmbeddedKafka(partitions = 1, topics = {
        "ccr-payments-test",
        "ccr-withdrawals-test",
        "ccr-withdrawal-sessions-test"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=ccr-kafka-it",
        "ccr.kafka.topics.payments.id=ccr-payments-test",
        "ccr.kafka.topics.payments.enabled=true",
        "ccr.kafka.topics.withdrawals.id=ccr-withdrawals-test",
        "ccr.kafka.topics.withdrawals.enabled=true",
        "ccr.kafka.topics.withdrawal-sessions.id=ccr-withdrawal-sessions-test",
        "ccr.kafka.topics.withdrawal-sessions.enabled=true"
})
class KafkaListenerIntegrationTest extends AbstractReportingIntegrationTest {

    private static final Duration LISTENER_TIMEOUT = Duration.ofSeconds(15);
    private static final ThriftSerializer THRIFT_SERIALIZER = new ThriftSerializer();

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @BeforeEach
    void waitForKafkaListenersAssignment() {
        for (MessageListenerContainer listenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(listenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @Test
    void paymentTopicMessageIsConsumedAndPersisted() throws Exception {
        sendBatch("ccr-payments-test", SerializedIngestionEventFixtures.paymentEvents());

        Map<String, Object> row = waitForRow(
                """
                SELECT status, provider_id, terminal_id, trx_id, finalized_at
                FROM ccr.payment_txn_current
                WHERE invoice_id = ? AND payment_id = ?
                """,
                current -> "captured".equals(current.get("status")) && current.get("trx_id") != null,
                SerializedIngestionEventFixtures.PAYMENT_INVOICE_ID,
                SerializedIngestionEventFixtures.PAYMENT_ID
        );

        assertThat(row.get("status")).isEqualTo("captured");
        assertThat(row.get("provider_id")).isEqualTo("100");
        assertThat(row.get("terminal_id")).isEqualTo("200");
        assertThat(row.get("trx_id")).isEqualTo("trx-payment-1");
        assertThat(((Timestamp) row.get("finalized_at")).toLocalDateTime())
                .isEqualTo(LocalDateTime.parse("2026-01-01T00:04:00"));
    }

    @Test
    void withdrawalTopicsMessagesAreConsumedAndPersisted() throws Exception {
        sendBatch("ccr-withdrawals-test", SerializedIngestionEventFixtures.withdrawalEvents());
        sendBatch("ccr-withdrawal-sessions-test", SerializedIngestionEventFixtures.withdrawalSessionEvents());

        Map<String, Object> row = waitForRow(
                """
                SELECT status, provider_id, terminal_id, fee, trx_id
                FROM ccr.withdrawal_txn_current
                WHERE withdrawal_id = ?
                """,
                current -> "succeeded".equals(current.get("status")) && current.get("trx_id") != null,
                SerializedIngestionEventFixtures.WITHDRAWAL_ID
        );

        assertThat(row.get("status")).isEqualTo("succeeded");
        assertThat(row.get("provider_id")).isEqualTo("300");
        assertThat(row.get("terminal_id")).isEqualTo("400");
        assertThat(row.get("fee")).isEqualTo(20L);
        assertThat(row.get("trx_id")).isEqualTo("trx-withdrawal-1");
    }

    private void sendBatch(String topic, List<MachineEvent> machineEvents) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties())) {
            for (MachineEvent machineEvent : machineEvents) {
                SinkEvent sinkEvent = new SinkEvent();
                sinkEvent.setEvent(machineEvent);
                byte[] payload = THRIFT_SERIALIZER.serialize("", sinkEvent);
                producer.send(new ProducerRecord<>(topic, machineEvent.getSourceId(), payload)).get();
            }
            producer.flush();
        }
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        return properties;
    }

    private Map<String, Object> waitForRow(
            String sql,
            Predicate<Map<String, Object>> predicate,
            Object... args
    ) throws InterruptedException {
        Instant deadline = Instant.now().plus(LISTENER_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, args);
            if (!rows.isEmpty() && predicate.test(rows.getFirst())) {
                return rows.getFirst();
            }
            Thread.sleep(200L);
        }
        return jdbcTemplate.queryForList(sql, args).stream()
                .filter(predicate)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Kafka listener did not reach expected row state within " + LISTENER_TIMEOUT));
    }
}
