package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.ingestion.PaymentIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalSessionIngestionService;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.SerializedIngestionEventFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет разбор сериализованных событий и то, как ingestion переносит их в current-state таблицы.
 */
class IngestionSerializedEventsIntegrationTest extends AbstractReportingIntegrationTest {

    @Autowired
    private PaymentIngestionService paymentIngestionService;

    @Autowired
    private WithdrawalIngestionService withdrawalIngestionService;

    @Autowired
    private WithdrawalSessionIngestionService withdrawalSessionIngestionService;

    @Test
    void paymentEventsAreParsedFromSerializedPayloadAndProjectedIntoCurrentState() {
        paymentIngestionService.handleEvents(SerializedIngestionEventFixtures.paymentEvents());

        Map<String, Object> row = jdbcTemplate.queryForMap(
                """
                        SELECT status, provider_id, terminal_id, amount, fee, trx_id, rrn, approval_code, finalized_at
                        FROM ccr.payment_txn_current
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                SerializedIngestionEventFixtures.PAYMENT_INVOICE_ID,
                SerializedIngestionEventFixtures.PAYMENT_ID
        );

        assertThat(row.get("status")).isEqualTo("captured");
        assertThat(row.get("provider_id")).isEqualTo("100");
        assertThat(row.get("terminal_id")).isEqualTo("200");
        assertThat(row.get("amount")).isEqualTo(1000L);
        assertThat(row.get("fee")).isEqualTo(10L);
        assertThat(row.get("trx_id")).isEqualTo("trx-payment-1");
        assertThat(row.get("rrn")).isEqualTo("rrn-payment-1");
        assertThat(row.get("approval_code")).isEqualTo("approval-payment-1");
        assertThat(((Timestamp) row.get("finalized_at")).toLocalDateTime())
                .isEqualTo(LocalDateTime.parse("2026-01-01T00:04:00"));
    }

    @Test
    void withdrawalEventsAreParsedFromSerializedPayloadAndProjectedIntoCurrentState() {
        withdrawalIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalEvents());
        withdrawalSessionIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalSessionEvents());

        Map<String, Object> row = jdbcTemplate.queryForMap(
                """
                        SELECT status, provider_id, terminal_id, amount, fee, trx_id, wallet_id,
                               original_amount, original_currency, converted_amount, provider_amount, provider_currency
                        FROM ccr.withdrawal_txn_current
                        WHERE withdrawal_id = ?
                        """,
                SerializedIngestionEventFixtures.WITHDRAWAL_ID
        );

        assertThat(row.get("status")).isEqualTo("succeeded");
        assertThat(row.get("provider_id")).isEqualTo("300");
        assertThat(row.get("terminal_id")).isEqualTo("400");
        assertThat(row.get("amount")).isEqualTo(1000L);
        assertThat(row.get("fee")).isEqualTo(20L);
        assertThat(row.get("trx_id")).isEqualTo("trx-withdrawal-1");
        assertThat(row.get("wallet_id")).isEqualTo("wallet-serialized");
        assertThat(row.get("original_amount")).isEqualTo(1200L);
        assertThat(row.get("original_currency")).isEqualTo("USD");
        assertThat(row.get("converted_amount")).isEqualTo(1000L);
        assertThat(row.get("provider_amount")).isEqualTo(1000L);
        assertThat(row.get("provider_currency")).isEqualTo("RUB");
    }
}
