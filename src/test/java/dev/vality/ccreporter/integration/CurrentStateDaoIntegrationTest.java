package dev.vality.ccreporter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.vality.ccreporter.dao.PaymentCurrentDao;
import dev.vality.ccreporter.dao.WithdrawalCurrentDao;
import dev.vality.ccreporter.dao.WithdrawalSessionBindingDao;
import dev.vality.ccreporter.ingestion.PaymentCurrentUpdate;
import dev.vality.ccreporter.ingestion.WithdrawalCurrentUpdate;
import dev.vality.ccreporter.ingestion.WithdrawalSessionBindingUpdate;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class CurrentStateDaoIntegrationTest extends AbstractReportingIntegrationTest {

    @Autowired
    private PaymentCurrentDao paymentCurrentDao;

    @Autowired
    private WithdrawalCurrentDao withdrawalCurrentDao;

    @Autowired
    private WithdrawalSessionBindingDao withdrawalSessionBindingDao;

    @Test
    void paymentUpsertIsMonotonicAndKeepsFirstFinalizedAt() {
        Instant finalizedAt = Instant.parse("2026-01-01T10:10:00Z");
        Instant laterFinalizedAt = Instant.parse("2026-01-01T10:20:00Z");

        paymentCurrentDao.upsert(paymentUpdate(10L, "captured", finalizedAt));
        paymentCurrentDao.upsert(paymentUpdate(11L, "refunded", laterFinalizedAt));
        paymentCurrentDao.upsert(paymentUpdate(9L, "pending", null));

        Map<String, Object> row = jdbcTemplate.queryForMap(
                """
                SELECT domain_event_id, status, finalized_at
                FROM ccr.payment_txn_current
                WHERE invoice_id = 'invoice-1' AND payment_id = 'payment-1'
                """
        );

        assertThat(row.get("domain_event_id")).isEqualTo(11L);
        assertThat(row.get("status")).isEqualTo("refunded");
        assertThat(((Timestamp) row.get("finalized_at")).toLocalDateTime())
                .isEqualTo(LocalDateTime.ofInstant(finalizedAt, ZoneOffset.UTC));
    }

    @Test
    void withdrawalSessionBindingIsMonotonicAndResolvesWithdrawalId() {
        withdrawalSessionBindingDao.upsert(new WithdrawalSessionBindingUpdate(
                "session-1",
                "withdrawal-1",
                20L,
                Instant.parse("2026-01-01T00:00:00Z")
        ));
        withdrawalSessionBindingDao.upsert(new WithdrawalSessionBindingUpdate(
                "session-1",
                "withdrawal-stale",
                19L,
                Instant.parse("2025-12-31T23:59:00Z")
        ));

        assertThat(withdrawalSessionBindingDao.findWithdrawalId("session-1")).contains("withdrawal-1");
    }

    @Test
    void withdrawalUpsertIsMonotonicAndKeepsFirstFinalizedAt() {
        Instant finalizedAt = Instant.parse("2026-01-01T11:10:00Z");
        Instant laterFinalizedAt = Instant.parse("2026-01-01T11:20:00Z");

        withdrawalCurrentDao.upsert(withdrawalUpdate(30L, "succeeded", finalizedAt, "trx-1"));
        withdrawalCurrentDao.upsert(withdrawalUpdate(31L, "failed", laterFinalizedAt, "trx-2"));
        withdrawalCurrentDao.upsert(withdrawalUpdate(29L, "pending", null, "trx-stale"));

        Map<String, Object> row = jdbcTemplate.queryForMap(
                """
                SELECT domain_event_id, status, finalized_at, trx_id
                FROM ccr.withdrawal_txn_current
                WHERE withdrawal_id = 'withdrawal-1'
                """
        );

        assertThat(row.get("domain_event_id")).isEqualTo(31L);
        assertThat(row.get("status")).isEqualTo("failed");
        assertThat(row.get("trx_id")).isEqualTo("trx-2");
        assertThat(((Timestamp) row.get("finalized_at")).toLocalDateTime())
                .isEqualTo(LocalDateTime.ofInstant(finalizedAt, ZoneOffset.UTC));
    }

    private PaymentCurrentUpdate paymentUpdate(long eventId, String status, Instant finalizedAt) {
        return new PaymentCurrentUpdate(
                "invoice-1",
                "payment-1",
                eventId,
                Instant.parse("2026-01-01T10:00:00Z").plusSeconds(eventId),
                "party-1",
                "shop-1",
                "Shop One",
                Instant.parse("2026-01-01T10:00:00Z"),
                finalizedAt,
                status,
                "provider-1",
                null,
                "terminal-1",
                null,
                1000L,
                10L,
                "RUB",
                "trx-1",
                "external-1",
                "rrn-1",
                "approval-1",
                "bank_card",
                1000L,
                "RUB",
                1000L,
                BigDecimal.ONE,
                1000L,
                "RUB"
        );
    }

    private WithdrawalCurrentUpdate withdrawalUpdate(long eventId, String status, Instant finalizedAt, String trxId) {
        return new WithdrawalCurrentUpdate(
                "withdrawal-1",
                eventId,
                Instant.parse("2026-01-01T11:00:00Z").plusSeconds(eventId),
                "party-1",
                "wallet-1",
                "Wallet One",
                "destination-1",
                Instant.parse("2026-01-01T11:00:00Z"),
                finalizedAt,
                status,
                "provider-1",
                null,
                "terminal-1",
                null,
                2000L,
                20L,
                "RUB",
                trxId,
                "external-1",
                "failure",
                "reason",
                "sub",
                2100L,
                "USD",
                2000L,
                new BigDecimal("0.9523809524"),
                2000L,
                "RUB"
        );
    }
}
