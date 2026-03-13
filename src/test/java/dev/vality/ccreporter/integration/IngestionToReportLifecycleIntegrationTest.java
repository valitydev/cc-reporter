package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.ingestion.PaymentIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalSessionIngestionService;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.fixture.SerializedIngestionEventFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Сквозной сценарий от ingestion до готового отчёта, чтобы вся цепочка проверялась одним прогоном.
 */
class IngestionToReportLifecycleIntegrationTest extends AbstractReportingIntegrationTest {

    @Autowired
    private PaymentIngestionService paymentIngestionService;

    @Autowired
    private WithdrawalIngestionService withdrawalIngestionService;

    @Autowired
    private WithdrawalSessionIngestionService withdrawalSessionIngestionService;

    @Test
    void paymentsReportLifecycleRunsFromIngestionToPresignedUrl() throws Exception {
        paymentIngestionService.handleEvents(SerializedIngestionEventFixtures.paymentEvents());

        var reportId = reportingHandler.createReport(paymentsLifecycleRequest());
        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );
        var url =
                reportingHandler.generatePresignedUrl(new GeneratePresignedUrlRequest(report.getFile().getFileId()));

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains(
                "created_date,created_time,finalized_date,finalized_time,invoice_id,payment_id,status,amount,currency"
        );
        assertThat(csv).contains(
                SerializedIngestionEventFixtures.PAYMENT_INVOICE_ID +
                        "," +
                        SerializedIngestionEventFixtures.PAYMENT_ID +
                        ",captured,10.00,RUB"
        );
        assertThat(csv).contains("trx-payment-1");
        assertThat(url).isEqualTo("https://download.example/" + report.getFile().getFileId());
        assertThat(reportingHandler.getReports(new GetReportsRequest()).getReports())
                .extracting(Report::getReportId)
                .contains(reportId);
    }

    @Test
    void realPaymentFixtureRunsThroughIngestionAndReportLifecycle() throws Exception {
        paymentIngestionService.handleEvents(SerializedIngestionEventFixtures.realPaymentEvents());

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT status, provider_id, terminal_id, amount, currency, trx_id, external_id,
                               payment_tool_type, finalized_at
                        FROM ccr.payment_txn_current
                        WHERE invoice_id = ? AND payment_id = ?
                        """,
                SerializedIngestionEventFixtures.REAL_PAYMENT_INVOICE_ID,
                SerializedIngestionEventFixtures.REAL_PAYMENT_ID
        );
        var reportId = reportingHandler.createReport(realPaymentsLifecycleRequest());
        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-03-13T00:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(row.get("status")).isEqualTo("captured");
        assertThat(row.get("provider_id")).isEqualTo("254");
        assertThat(row.get("terminal_id")).isEqualTo("2551");
        assertThat(row.get("amount")).isEqualTo(425000L);
        assertThat(row.get("currency")).isEqualTo("KZT");
        assertThat(row.get("trx_id")).isEqualTo("test-provider-trx-1");
        assertThat(row.get("external_id")).isEqualTo("test-external-1");
        assertThat(row.get("payment_tool_type")).isEqualTo("bank_card");
        assertThat(row.get("finalized_at")).isNotNull();
        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains(
                SerializedIngestionEventFixtures.REAL_PAYMENT_INVOICE_ID +
                        "," +
                        SerializedIngestionEventFixtures.REAL_PAYMENT_ID +
                        ",captured,4250.00,KZT"
        );
        assertThat(csv).contains("test-provider-trx-1");
    }

    @Test
    void withdrawalsReportLifecycleRunsFromIngestionToPresignedUrl() throws Exception {
        withdrawalIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalEvents());
        withdrawalSessionIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalSessionEvents());

        var reportId = reportingHandler.createReport(withdrawalsLifecycleRequest());
        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );
        var url =
                reportingHandler.generatePresignedUrl(new GeneratePresignedUrlRequest(report.getFile().getFileId()));

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains(
                "created_date,created_time,finalized_date,finalized_time,withdrawal_id,status,amount,currency"
        );
        assertThat(csv).contains(SerializedIngestionEventFixtures.WITHDRAWAL_ID + ",succeeded,10.00,RUB");
        assertThat(csv).contains("trx-withdrawal-1");
        assertThat(url).isEqualTo("https://download.example/" + report.getFile().getFileId());
        assertThat(reportingHandler.getReports(new GetReportsRequest()).getReports())
                .extracting(Report::getReportId)
                .contains(reportId);
    }

    @Test
    void realWithdrawalFixtureRunsThroughIngestionAndReportLifecycle() throws Exception {
        withdrawalIngestionService.handleEvents(SerializedIngestionEventFixtures.realWithdrawalEvents());

        var row = jdbcTemplate.queryForMap(
                """
                        SELECT status, provider_id, terminal_id, amount, fee, currency, wallet_id, external_id,
                               finalized_at
                        FROM ccr.withdrawal_txn_current
                        WHERE withdrawal_id = ?
                        """,
                SerializedIngestionEventFixtures.REAL_WITHDRAWAL_ID
        );
        var reportId = reportingHandler.createReport(realWithdrawalsLifecycleRequest());
        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-03-13T00:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(row.get("status")).isEqualTo("succeeded");
        assertThat(row.get("provider_id")).isEqualTo("518");
        assertThat(row.get("terminal_id")).isEqualTo("2465");
        assertThat(row.get("amount")).isEqualTo(2900000L);
        assertThat(row.get("fee")).isEqualTo(145000L);
        assertThat(row.get("currency")).isEqualTo("RUB");
        assertThat(row.get("wallet_id")).isEqualTo("3313");
        assertThat(row.get("external_id")).isEqualTo("test-withdrawal-external-1");
        assertThat(row.get("finalized_at")).isNotNull();
        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains(
                SerializedIngestionEventFixtures.REAL_WITHDRAWAL_ID + ",succeeded,29000.00,RUB"
        );
        assertThat(csv).contains("518");
        assertThat(csv).contains("2465");
    }

    private dev.vality.ccreporter.CreateReportRequest paymentsLifecycleRequest() {
        return ReportRequestFixtures.payments("ingestion-payments-lifecycle-1", new TimeRange(
                "2025-12-31T00:00:00Z",
                "2026-01-02T00:00:00Z"
        ));
    }

    private dev.vality.ccreporter.CreateReportRequest withdrawalsLifecycleRequest() {
        return ReportRequestFixtures.withdrawals("ingestion-withdrawals-lifecycle-1", new TimeRange(
                "2025-12-31T00:00:00Z",
                "2026-01-02T00:00:00Z"
        ));
    }

    private dev.vality.ccreporter.CreateReportRequest realPaymentsLifecycleRequest() {
        return ReportRequestFixtures.payments("ingestion-real-payments-lifecycle-1", new TimeRange(
                "2026-03-12T00:00:00Z",
                "2026-03-13T00:00:00Z"
        ));
    }

    private dev.vality.ccreporter.CreateReportRequest realWithdrawalsLifecycleRequest() {
        return ReportRequestFixtures.withdrawals("ingestion-real-withdrawals-lifecycle-1", new TimeRange(
                "2026-02-17T00:00:00Z",
                "2026-02-21T00:00:00Z"
        ));
    }
}
