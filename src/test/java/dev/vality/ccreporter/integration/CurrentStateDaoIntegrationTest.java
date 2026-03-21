package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.dao.DisplayNameLookupDao;
import dev.vality.ccreporter.dao.PaymentTxnCurrentDao;
import dev.vality.ccreporter.dao.WithdrawalSessionDao;
import dev.vality.ccreporter.dao.WithdrawalTxnCurrentDao;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.CurrentStateUpdateFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет, как DAO обновляют current state и что старые события не перетирают более свежие.
 */
class CurrentStateDaoIntegrationTest extends AbstractReportingIntegrationTest {

    @Autowired
    private PaymentTxnCurrentDao paymentTxnCurrentDao;

    @Autowired
    private WithdrawalTxnCurrentDao withdrawalTxnCurrentDao;

    @Autowired
    private WithdrawalSessionDao withdrawalSessionDao;

    @Autowired
    private DisplayNameLookupDao displayNameLookupDao;

    @Test
    void paymentUpsertIsMonotonic() {
        var finalizedAt = Instant.parse("2026-01-01T10:10:00Z");
        var laterFinalizedAt = Instant.parse("2026-01-01T10:20:00Z");

        paymentTxnCurrentDao.upsert(CurrentStateUpdateFixtures.paymentUpdate(10L, "captured", finalizedAt));
        paymentTxnCurrentDao.upsert(CurrentStateUpdateFixtures.paymentUpdate(11L, "refunded", laterFinalizedAt));
        paymentTxnCurrentDao.upsert(CurrentStateUpdateFixtures.paymentUpdate(9L, "pending", null));

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT domain_event_id, status, finalized_at
                        FROM ccr.payment_txn_current
                        WHERE invoice_id = 'invoice-1' AND payment_id = 'payment-1'
                        """
        );

        assertThat(row.get("domain_event_id")).isEqualTo(11L);
        assertThat(row.get("status")).isEqualTo("refunded");
        assertThat(((Timestamp) Objects.requireNonNull(row.get("finalized_at"))).toLocalDateTime())
                .isEqualTo(LocalDateTime.ofInstant(laterFinalizedAt, ZoneOffset.UTC));
    }

    @Test
    void withdrawalSessionIsMonotonic() {
        withdrawalSessionDao.upsert(CurrentStateUpdateFixtures.withdrawalSessionUpdate(
                "session-1",
                "withdrawal-1",
                20L,
                Instant.parse("2026-01-01T00:00:00Z"),
                null
        ));
        withdrawalSessionDao.upsert(CurrentStateUpdateFixtures.withdrawalSessionUpdate(
                "session-1",
                "withdrawal-stale",
                19L,
                Instant.parse("2025-12-31T23:59:00Z"),
                null
        ));

        var row = jdbcTemplate.queryForMap(
                "SELECT withdrawal_id FROM ccr.withdrawal_session WHERE session_id = 'session-1'"
        );
        assertThat(row.get("withdrawal_id")).isEqualTo("withdrawal-1");
    }

    @Test
    void withdrawalSessionTrxIdIsSet() {
        withdrawalSessionDao.upsert(CurrentStateUpdateFixtures.withdrawalSessionUpdate(
                "session-2",
                "withdrawal-2",
                30L,
                Instant.parse("2026-01-01T00:00:00Z"),
                null
        ));
        withdrawalSessionDao.upsert(CurrentStateUpdateFixtures.withdrawalSessionUpdate(
                "session-2",
                "withdrawal-2",
                31L,
                Instant.parse("2026-01-01T00:01:00Z"),
                "trx-123"
        ));

        var row = jdbcTemplate.queryForMap(
                "SELECT trx_id, trx_search FROM ccr.withdrawal_session WHERE session_id = 'session-2'"
        );
        assertThat(row.get("trx_id")).isEqualTo("trx-123");
        assertThat(row.get("trx_search")).isEqualTo("trx-123");
    }

    @Test
    void withdrawalUpsertIsMonotonic() {
        var finalizedAt = Instant.parse("2026-01-01T11:10:00Z");
        var laterFinalizedAt = Instant.parse("2026-01-01T11:20:00Z");

        withdrawalTxnCurrentDao.upsert(
                CurrentStateUpdateFixtures.withdrawalUpdate(30L, "succeeded", finalizedAt)
        );
        withdrawalTxnCurrentDao.upsert(
                CurrentStateUpdateFixtures.withdrawalUpdate(31L, "failed", laterFinalizedAt)
        );
        withdrawalTxnCurrentDao.upsert(CurrentStateUpdateFixtures.withdrawalUpdate(29L, "pending", null));

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT domain_event_id, status, finalized_at
                        FROM ccr.withdrawal_txn_current
                        WHERE withdrawal_id = 'withdrawal-1'
                        """
        );

        assertThat(row.get("domain_event_id")).isEqualTo(31L);
        assertThat(row.get("status")).isEqualTo("failed");
        assertThat(((Timestamp) Objects.requireNonNull(row.get("finalized_at"))).toLocalDateTime())
                .isEqualTo(LocalDateTime.ofInstant(laterFinalizedAt, ZoneOffset.UTC));
    }

    @Test
    void displayNameLookupUpsertOverwritesExistingDominantName() {
        displayNameLookupDao.upsertShop("shop-1", "Shop One");
        displayNameLookupDao.upsertShop("shop-1", "Shop Uno");
        displayNameLookupDao.upsertProvider("provider-1", "Provider One");
        displayNameLookupDao.upsertTerminal("terminal-1", "Terminal One");
        displayNameLookupDao.upsertWallet("wallet-1", "Wallet One");

        assertThat(jdbcTemplate.queryForObject(
                "SELECT shop_name FROM ccr.shop_lookup WHERE shop_id = 'shop-1'",
                String.class
        )).isEqualTo("Shop Uno");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT shop_search FROM ccr.shop_lookup WHERE shop_id = 'shop-1'",
                String.class
        )).isEqualTo("shop-1 shop uno");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT provider_name FROM ccr.provider_lookup WHERE provider_id = 'provider-1'",
                String.class
        )).isEqualTo("Provider One");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT provider_search FROM ccr.provider_lookup WHERE provider_id = 'provider-1'",
                String.class
        )).isEqualTo("provider-1 provider one");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT terminal_name FROM ccr.terminal_lookup WHERE terminal_id = 'terminal-1'",
                String.class
        )).isEqualTo("Terminal One");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT terminal_search FROM ccr.terminal_lookup WHERE terminal_id = 'terminal-1'",
                String.class
        )).isEqualTo("terminal-1 terminal one");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT wallet_name FROM ccr.wallet_lookup WHERE wallet_id = 'wallet-1'",
                String.class
        )).isEqualTo("Wallet One");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT wallet_search FROM ccr.wallet_lookup WHERE wallet_id = 'wallet-1'",
                String.class
        )).isEqualTo("wallet-1 wallet one");
    }
}
