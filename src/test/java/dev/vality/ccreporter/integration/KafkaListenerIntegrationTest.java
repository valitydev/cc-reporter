package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.SerializedIngestionEventFixtures;
import dev.vality.ccreporter.integration.support.KafkaIntegrationTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет, что Kafka listeners подхватывают сообщения из тестовых топиков и обновляют current-state таблицы.
 */
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

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @BeforeEach
    void waitForKafkaListenersAssignment() {
        KafkaIntegrationTestSupport.waitForAssignments(kafkaListenerEndpointRegistry, embeddedKafkaBroker);
    }

    @Test
    void paymentTopicMessageIsConsumedAndPersisted() throws Exception {
        KafkaIntegrationTestSupport.sendBatch(
                embeddedKafkaBroker,
                "ccr-payments-test",
                SerializedIngestionEventFixtures.paymentEvents()
        );

        var row = KafkaIntegrationTestSupport.waitForRow(
                jdbcTemplate,
                LISTENER_TIMEOUT,
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
        KafkaIntegrationTestSupport.sendBatch(
                embeddedKafkaBroker,
                "ccr-withdrawals-test",
                SerializedIngestionEventFixtures.withdrawalEvents()
        );
        KafkaIntegrationTestSupport.sendBatch(
                embeddedKafkaBroker,
                "ccr-withdrawal-sessions-test",
                SerializedIngestionEventFixtures.withdrawalSessionEvents()
        );

        var row = KafkaIntegrationTestSupport.waitForRow(
                jdbcTemplate,
                LISTENER_TIMEOUT,
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
}
