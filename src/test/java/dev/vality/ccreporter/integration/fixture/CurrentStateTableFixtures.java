package dev.vality.ccreporter.integration.fixture;

import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Наполняет current-state таблицы тестовыми строками там, где нужен готовый срез данных без участия ingestion.
 */
public final class CurrentStateTableFixtures {

    private CurrentStateTableFixtures() {
    }

    public static void insertPaymentRow(
            JdbcTemplate jdbcTemplate,
            String invoiceId,
            String paymentId,
            Instant createdAt,
            Instant finalizedAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO ccr.payment_txn_current (
                            invoice_id, payment_id, domain_event_id, domain_event_created_at,
                            party_id, shop_id, created_at, finalized_at, status,
                            provider_id, terminal_id, amount,
                            fee, currency, trx_id, external_id, rrn, approval_code,
                            payment_tool_type, original_amount, original_currency,
                            converted_amount, exchange_rate_internal, provider_amount,
                            provider_currency, trx_search
                        )
                        VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                invoiceId,
                paymentId,
                1L,
                Timestamp.from(createdAt),
                "party-1",
                "shop-1",
                Timestamp.from(createdAt),
                finalizedAt == null ? null : Timestamp.from(finalizedAt),
                "captured",
                "provider-1",
                "terminal-1",
                1000L,
                10L,
                "RUB",
                "trx-1",
                "external-1",
                "rrn-1",
                "approval-1",
                "bank_card",
                1100L,
                "USD",
                1000L,
                new BigDecimal("1.1000000000"),
                990L,
                "EUR",
                "trx-1"
        );
    }

    public static void insertWithdrawalRow(
            JdbcTemplate jdbcTemplate,
            String withdrawalId,
            Instant createdAt,
            Instant finalizedAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO ccr.withdrawal_txn_current (
                            withdrawal_id, domain_event_id, domain_event_created_at,
                            party_id, wallet_id, destination_id, created_at,
                            finalized_at, status, provider_id, terminal_id, amount, fee, currency, trx_id, external_id,
                            error_code, error_reason, error_sub_failure, original_amount,
                            original_currency, converted_amount, exchange_rate_internal,
                            provider_amount, provider_currency, trx_search
                        )
                        VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                withdrawalId,
                1L,
                Timestamp.from(createdAt),
                "party-1",
                "wallet-1",
                "destination-1",
                Timestamp.from(createdAt),
                finalizedAt == null ? null : Timestamp.from(finalizedAt),
                "succeeded",
                "provider-1",
                "terminal-1",
                2000L,
                20L,
                "RUB",
                "trx-w-1",
                "external-w-1",
                null,
                null,
                null,
                2100L,
                "USD",
                2000L,
                new BigDecimal("1.0500000000"),
                1990L,
                "EUR",
                "trx-w-1"
        );
    }
}
