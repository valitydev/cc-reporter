package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.ingestion.payment.PaymentIngestionService;
import dev.vality.ccreporter.ingestion.withdrawal.WithdrawalIngestionService;
import dev.vality.ccreporter.ingestion.withdrawal.session.WithdrawalSessionIngestionService;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.SerializedIngestionEventFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;

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

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT status, provider_id, terminal_id, amount, fee, trx_id, rrn, approval_code, finalized_at,
                               original_amount, original_currency, converted_amount, exchange_rate_internal,
                               provider_amount, provider_currency
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
        assertThat(row.get("original_amount")).isEqualTo(1000L);
        assertThat(row.get("original_currency")).isEqualTo("RUB");
        assertThat(row.get("converted_amount")).isEqualTo(900L);
        assertThat(row.get("exchange_rate_internal")).isEqualTo(new java.math.BigDecimal("0.9000000000"));
        assertThat(row.get("provider_amount")).isEqualTo(900L);
        assertThat(row.get("provider_currency")).isEqualTo("EUR");
        assertThat(((Timestamp) Objects.requireNonNull(row.get("finalized_at"))).toLocalDateTime())
                .isEqualTo(LocalDateTime.parse("2026-01-01T00:04:00"));
    }

    @Test
    void paymentProxyStateFallbackPopulatesTrxIdWhenTransactionBoundIsAbsent() {
        paymentIngestionService.handleEvents(SerializedIngestionEventFixtures.paymentProxyStateFallbackEvents());

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT status, trx_id
                        FROM ccr.payment_txn_current
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                SerializedIngestionEventFixtures.PAYMENT_INVOICE_ID,
                SerializedIngestionEventFixtures.PAYMENT_ID
        );

        assertThat(row.get("status")).isEqualTo("captured");
        assertThat(row.get("trx_id")).isEqualTo("trx-from-proxy-state-1");
    }

    @Test
    void failedPaymentStatusStoresPackedErrorSummary() {
        paymentIngestionService.handleEvents(SerializedIngestionEventFixtures.failedPaymentEvents());

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT status, error_summary
                        FROM ccr.payment_txn_current
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                SerializedIngestionEventFixtures.PAYMENT_INVOICE_ID,
                SerializedIngestionEventFixtures.PAYMENT_ID
        );

        assertThat(row.get("status")).isEqualTo("failed");
        assertThat(row.get("error_summary"))
                .isEqualTo("authorization_failed:payment_tool_rejected | 'FAILED: REFUSED' - 'Transaction failed'");
    }

    @Test
    void legacyPaymentShapeWithOwnerIdAndShopIdStillProjectsIntoCurrentState() {
        paymentIngestionService.handleEvents(SerializedIngestionEventFixtures.legacyPaymentEvents());

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT party_id, shop_id, status, amount, currency, payment_tool_type
                        FROM ccr.payment_txn_current
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                SerializedIngestionEventFixtures.LEGACY_PAYMENT_INVOICE_ID,
                SerializedIngestionEventFixtures.LEGACY_PAYMENT_ID
        );

        assertThat(row.get("party_id")).isEqualTo("test-party-1");
        assertThat(row.get("shop_id")).isEqualTo("test-shop-1");
        assertThat(row.get("status")).isEqualTo("failed");
        assertThat(row.get("amount")).isEqualTo(100000L);
        assertThat(row.get("currency")).isEqualTo("KZT");
        assertThat(row.get("payment_tool_type")).isEqualTo("payment_terminal");
    }

    @Test
    void withdrawalEventsAreParsedFromSerializedPayloadAndProjectedIntoCurrentState() {
        withdrawalIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalEvents());
        withdrawalSessionIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalSessionEvents());

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT status, provider_id, terminal_id, amount, fee, trx_id, wallet_id,
                               original_amount, original_currency, provider_amount, provider_currency
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
        assertThat(row.get("provider_amount")).isEqualTo(1000L);
        assertThat(row.get("provider_currency")).isEqualTo("RUB");
    }
}
