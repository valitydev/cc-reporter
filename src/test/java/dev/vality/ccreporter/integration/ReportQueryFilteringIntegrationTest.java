package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.PaymentsSearchFilter;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.WithdrawalsSearchFilter;
import dev.vality.ccreporter.dao.DisplayNameLookupDao;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.CurrentStateTableFixtures;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.fixture.SerializedIngestionEventFixtures;
import dev.vality.ccreporter.ingestion.PaymentIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalIngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет, что фильтры в запросе на отчёт реально меняют выборку, а не остаются декоративными полями.
 */
class ReportQueryFilteringIntegrationTest extends AbstractReportingIntegrationTest {

    @Autowired
    private PaymentIngestionService paymentIngestionService;

    @Autowired
    private WithdrawalIngestionService withdrawalIngestionService;

    @Autowired
    private DisplayNameLookupDao displayNameLookupDao;

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
        var filter = new PaymentsSearchFilter();
        filter.setShopTerm("shop one");
        filter.setTrxTerm("trx-1");
        request.getQuery().getPayments().setFilter(filter);
        var reportId = reportingHandler.createReport(request);

        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
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
        var filter = new WithdrawalsSearchFilter();
        filter.setWalletTerm("wallet one");
        filter.setTrxTerm("trx-w-1");
        request.getQuery().getWithdrawals().setFilter(filter);
        var reportId = reportingHandler.createReport(request);

        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("withdrawal-filter-1,succeeded,20.00,RUB,trx-w-1");
        assertThat(csv).doesNotContain("withdrawal-filter-2,succeeded,20.00,RUB,trx-w-2");
    }

    @Test
    void paymentsNameFiltersUseLocalLookupWhenCurrentStateNamesAreMissing() throws Exception {
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-lookup-1",
                "payment-lookup-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        jdbcTemplate.update(
                """
                        UPDATE ccr.payment_txn_current
                        SET shop_name = NULL, provider_name = NULL, terminal_name = NULL,
                            shop_search = NULL, provider_search = NULL, terminal_search = NULL
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                "invoice-lookup-1",
                "payment-lookup-1"
        );
        displayNameLookupDao.upsertShop("shop-1", "Lookup Shop");
        displayNameLookupDao.upsertProvider("provider-1", "Lookup Provider");
        displayNameLookupDao.upsertTerminal("terminal-1", "Lookup Terminal");

        final var request = ReportRequestFixtures.payments("payments-lookup-1");
        var filter = new PaymentsSearchFilter();
        filter.setShopTerm("lookup shop");
        filter.setProviderTerm("lookup provider");
        filter.setTerminalTerm("lookup terminal");
        request.getQuery().getPayments().setFilter(filter);
        var reportId = reportingHandler.createReport(request);

        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("invoice-lookup-1,payment-lookup-1");
    }

    @Test
    void withdrawalsNameFiltersUseLocalLookupWhenCurrentStateNamesAreMissing() throws Exception {
        CurrentStateTableFixtures.insertWithdrawalRow(
                jdbcTemplate,
                "withdrawal-lookup-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        jdbcTemplate.update(
                """
                        UPDATE ccr.withdrawal_txn_current
                        SET wallet_name = NULL, provider_name = NULL, terminal_name = NULL,
                            wallet_search = NULL, provider_search = NULL, terminal_search = NULL
                        WHERE withdrawal_id = ?
                        """,
                "withdrawal-lookup-1"
        );
        displayNameLookupDao.upsertWallet("wallet-1", "Lookup Wallet");
        displayNameLookupDao.upsertProvider("provider-1", "Lookup Provider");
        displayNameLookupDao.upsertTerminal("terminal-1", "Lookup Terminal");

        final var request = ReportRequestFixtures.withdrawals("withdrawals-lookup-1");
        var filter = new WithdrawalsSearchFilter();
        filter.setWalletTerm("lookup wallet");
        filter.setProviderTerm("lookup provider");
        filter.setTerminalTerm("lookup terminal");
        request.getQuery().getWithdrawals().setFilter(filter);
        var reportId = reportingHandler.createReport(request);

        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("withdrawal-lookup-1,succeeded,20.00,RUB,trx-w-1");
    }

    @Test
    void paymentCollectionFiltersCanNarrowLargeFixtureSetToSingleRow() throws Exception {
        paymentIngestionService.handleEvents(SerializedIngestionEventFixtures.paymentCollectionEvents());

        var request = ReportRequestFixtures.payments(
                "payments-collection-filter-1",
                ReportRequestFixtures.timeRange("2025-11-01T00:00:00Z", "2026-03-14T00:00:00Z")
        );
        request.getQuery().getPayments().setPartyIds(List.of("test-party-1"));
        request.getQuery().getPayments().setProviderIds(List.of("254"));
        request.getQuery().getPayments().setTerminalIds(List.of("2551"));
        request.getQuery().getPayments().setTrxIds(List.of("test-provider-trx-1"));
        request.getQuery().getPayments().setStatuses(List.of("captured"));
        var filter = new PaymentsSearchFilter();
        filter.setTrxTerm("test-provider-trx-1");
        request.getQuery().getPayments().setFilter(filter);
        var reportId = reportingHandler.createReport(request);

        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-03-14T00:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("2EnbPdxImPo,1,captured");
        assertThat(csv).doesNotContain("2EfF8NQk30a,1,");
        assertThat(csv).doesNotContain("test-invoice-1,1,");
    }

    @Test
    void withdrawalCollectionFiltersCanNarrowLargeFixtureSetToSingleRow() throws Exception {
        withdrawalIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalCollectionEvents());

        var request = ReportRequestFixtures.withdrawals(
                "withdrawals-collection-filter-1",
                ReportRequestFixtures.timeRange("2026-02-17T00:00:00Z", "2026-03-14T00:00:00Z")
        );
        request.getQuery().getWithdrawals().setWalletIds(List.of("3313"));
        request.getQuery().getWithdrawals().setProviderIds(List.of("518"));
        request.getQuery().getWithdrawals().setTerminalIds(List.of("2465"));
        request.getQuery().getWithdrawals().setStatuses(List.of("succeeded"));
        var reportId = reportingHandler.createReport(request);

        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-03-14T00:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("211890,succeeded,29000.00,RUB");
        assertThat(csv).doesNotContain("257060,succeeded,");
        assertThat(csv).doesNotContain("257085,succeeded,");
    }
}
