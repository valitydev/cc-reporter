package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.dao.PaymentCurrentDao;
import dev.vality.ccreporter.dao.DisplayNameLookupDao;
import dev.vality.ccreporter.dao.WithdrawalCurrentDao;
import dev.vality.ccreporter.dao.WithdrawalSessionBindingDao;
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
    private PaymentCurrentDao paymentCurrentDao;

    @Autowired
    private WithdrawalCurrentDao withdrawalCurrentDao;

    @Autowired
    private WithdrawalSessionBindingDao withdrawalSessionBindingDao;

    @Autowired
    private DisplayNameLookupDao displayNameLookupDao;

    @Test
    void paymentUpsertIsMonotonicAndKeepsFirstFinalizedAt() {
        var finalizedAt = Instant.parse("2026-01-01T10:10:00Z");
        var laterFinalizedAt = Instant.parse("2026-01-01T10:20:00Z");

        paymentCurrentDao.upsert(CurrentStateUpdateFixtures.paymentUpdate(10L, "captured", finalizedAt));
        paymentCurrentDao.upsert(CurrentStateUpdateFixtures.paymentUpdate(11L, "refunded", laterFinalizedAt));
        paymentCurrentDao.upsert(CurrentStateUpdateFixtures.paymentUpdate(9L, "pending", null));

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
                .isEqualTo(LocalDateTime.ofInstant(finalizedAt, ZoneOffset.UTC));
    }

    @Test
    void withdrawalSessionBindingIsMonotonicAndResolvesWithdrawalId() {
        withdrawalSessionBindingDao.upsert(CurrentStateUpdateFixtures.withdrawalSessionBindingUpdate(
                "session-1",
                "withdrawal-1",
                20L,
                Instant.parse("2026-01-01T00:00:00Z")
        ));
        withdrawalSessionBindingDao.upsert(CurrentStateUpdateFixtures.withdrawalSessionBindingUpdate(
                "session-1",
                "withdrawal-stale",
                19L,
                Instant.parse("2025-12-31T23:59:00Z")
        ));

        assertThat(withdrawalSessionBindingDao.findWithdrawalId("session-1")).contains("withdrawal-1");
    }

    @Test
    void withdrawalUpsertIsMonotonicAndKeepsFirstFinalizedAt() {
        var finalizedAt = Instant.parse("2026-01-01T11:10:00Z");
        var laterFinalizedAt = Instant.parse("2026-01-01T11:20:00Z");

        withdrawalCurrentDao.upsert(
                CurrentStateUpdateFixtures.withdrawalUpdate(30L, "succeeded", finalizedAt, "trx-1")
        );
        withdrawalCurrentDao.upsert(
                CurrentStateUpdateFixtures.withdrawalUpdate(31L, "failed", laterFinalizedAt, "trx-2")
        );
        withdrawalCurrentDao.upsert(CurrentStateUpdateFixtures.withdrawalUpdate(29L, "pending", null, "trx-stale"));

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT domain_event_id, status, finalized_at, trx_id
                        FROM ccr.withdrawal_txn_current
                        WHERE withdrawal_id = 'withdrawal-1'
                        """
        );

        assertThat(row.get("domain_event_id")).isEqualTo(31L);
        assertThat(row.get("status")).isEqualTo("failed");
        assertThat(row.get("trx_id")).isEqualTo("trx-2");
        assertThat(((Timestamp) Objects.requireNonNull(row.get("finalized_at"))).toLocalDateTime())
                .isEqualTo(LocalDateTime.ofInstant(finalizedAt, ZoneOffset.UTC));
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
