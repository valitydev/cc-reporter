package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.ingestion.payment.PaymentIngestionService;
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
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;

@EmbeddedKafka(partitions = 1, topics = "ccr-payments-retry-test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=ccr-kafka-retry-it",
        "ccr.kafka.consumer.error-backoff-interval-ms=100",
        "ccr.kafka.topics.payments.id=ccr-payments-retry-test",
        "ccr.kafka.topics.payments.enabled=true"
})
class KafkaListenerRetryIntegrationTest extends AbstractReportingIntegrationTest {

    private static final Duration LISTENER_TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @MockitoSpyBean
    private PaymentIngestionService paymentIngestionService;

    @BeforeEach
    void waitForKafkaListenersAssignment() {
        KafkaIntegrationTestSupport.waitForAssignments(kafkaListenerEndpointRegistry, embeddedKafkaBroker);
    }

    @Test
    void failedBatchIsRetriedAndCommittedOnlyAfterSuccessfulProcessing() throws Exception {
        var attempts = new AtomicInteger();
        doAnswer(invocation -> {
            if (attempts.getAndIncrement() == 0) {
                throw new IllegalStateException("synthetic payment batch failure");
            }
            return invocation.callRealMethod();
        }).when(paymentIngestionService).handleEvents(anyList());

        KafkaIntegrationTestSupport.sendBatch(
                embeddedKafkaBroker,
                "ccr-payments-retry-test",
                SerializedIngestionEventFixtures.paymentEvents()
        );

        var row = KafkaIntegrationTestSupport.waitForRow(
                jdbcTemplate,
                LISTENER_TIMEOUT,
                """
                        SELECT status, trx_id
                        FROM ccr.payment_txn_current
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                current -> "captured".equals(current.get("status")) && current.get("trx_id") != null,
                SerializedIngestionEventFixtures.PAYMENT_INVOICE_ID,
                SerializedIngestionEventFixtures.PAYMENT_ID
        );

        var rowCount = jdbcTemplate.queryForObject(
                """
                        SELECT count(*)
                        FROM ccr.payment_txn_current
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                Integer.class,
                SerializedIngestionEventFixtures.PAYMENT_INVOICE_ID,
                SerializedIngestionEventFixtures.PAYMENT_ID
        );

        assertThat(row.get("status")).isEqualTo("captured");
        assertThat(row.get("trx_id")).isEqualTo("trx-payment-1");
        assertThat(rowCount).isEqualTo(1);
        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
    }
}
