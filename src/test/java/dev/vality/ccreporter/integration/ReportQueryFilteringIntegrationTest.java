package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.CurrentStateTableFixtures;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет, что фильтры в запросе на отчёт реально меняют выборку, а не остаются декоративными полями.
 */
class ReportQueryFilteringIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void paymentsQueryFiltersExcludeNonMatchingRows() throws Exception {
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-filter-1",
                "payment-filter-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-filter-2",
                "payment-filter-2",
                Instant.parse("2026-01-01T10:01:00Z"),
                Instant.parse("2026-01-01T11:01:00Z")
        );
        jdbcTemplate.update(
                """
                        UPDATE ccr.payment_txn_current
                        SET shop_id = ?, shop_name = ?, shop_search = ?, trx_id = ?, trx_search = ?
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                "shop-2",
                "Other Shop",
                "other shop",
                "trx-2",
                "trx-2",
                "invoice-filter-2",
                "payment-filter-2"
        );

        var request = ReportRequestFixtures.payments("payments-filter-1");
        request.getQuery().getPayments().setShopIds(List.of("shop-1"));
        PaymentsSearchFilter filter = new PaymentsSearchFilter();
        filter.setShopTerm("shop one");
        filter.setTrxTerm("trx-1");
        request.getQuery().getPayments().setFilter(filter);
        long reportId = reportingHandler.createReport(request);

        boolean processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        String csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("invoice-filter-1,payment-filter-1");
        assertThat(csv).doesNotContain("invoice-filter-2,payment-filter-2");
    }

    @Test
    void withdrawalsQueryFiltersExcludeNonMatchingRows() throws Exception {
        CurrentStateTableFixtures.insertWithdrawalRow(
                jdbcTemplate,
                "withdrawal-filter-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        CurrentStateTableFixtures.insertWithdrawalRow(
                jdbcTemplate,
                "withdrawal-filter-2",
                Instant.parse("2026-01-01T10:01:00Z"),
                Instant.parse("2026-01-01T11:01:00Z")
        );
        jdbcTemplate.update(
                """
                        UPDATE ccr.withdrawal_txn_current
                        SET wallet_id = ?, wallet_name = ?, wallet_search = ?, trx_id = ?, trx_search = ?
                        WHERE withdrawal_id = ?
                        """,
                "wallet-2",
                "Other Wallet",
                "other wallet",
                "trx-w-2",
                "trx-w-2",
                "withdrawal-filter-2"
        );

        var request = ReportRequestFixtures.withdrawals("withdrawals-filter-1");
        request.getQuery().getWithdrawals().setWalletIds(List.of("wallet-1"));
        WithdrawalsSearchFilter filter = new WithdrawalsSearchFilter();
        filter.setWalletTerm("wallet one");
        filter.setTrxTerm("trx-w-1");
        request.getQuery().getWithdrawals().setFilter(filter);
        long reportId = reportingHandler.createReport(request);

        boolean processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        String csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("withdrawal-filter-1,succeeded,20.00,RUB,trx-w-1");
        assertThat(csv).doesNotContain("withdrawal-filter-2,succeeded,20.00,RUB,trx-w-2");
    }
}
