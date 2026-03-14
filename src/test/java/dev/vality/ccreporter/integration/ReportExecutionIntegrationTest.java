package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.CurrentStateTableFixtures;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет, как из current state собирается CSV и как готовый файл публикуется в storage.
 */
class ReportExecutionIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void paymentsReportIsBuiltAndPublishedEndToEnd() throws Exception {
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-1",
                "payment-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        var request = ReportRequestFixtures.payments("exec-payments-1");
        request.setTimezone("Asia/Krasnoyarsk");
        var reportId = reportingHandler.createReport(request);

        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csvLines = readCsvLines(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(report.getFile().getFilename()).isEqualTo("payments-report-" + reportId + ".csv");
        assertThat(report.getFile().getContentType()).isEqualTo("text/csv");
        assertThat(report.getDataSnapshotFixedAt()).isNotBlank();
        assertThat(csvLines).containsExactly(
                "created_date,created_time,finalized_date,finalized_time,invoice_id,payment_id,status," +
                        "amount,currency,trx_id,provider_id,terminal_id,shop_id,exchange_rate_internal," +
                        "provider_amount,provider_currency,converted_amount",
                "2026-01-01,17:00:00,2026-01-01,18:00:00,invoice-1,payment-1,captured,10.00,RUB,trx-1," +
                        "provider-1,terminal-1,shop-1,1.1000000000,9.90,EUR,10.00"
        );
    }

    @Test
    void withdrawalsReportIsBuiltAndPublishedEndToEnd() throws Exception {
        CurrentStateTableFixtures.insertWithdrawalRow(
                jdbcTemplate,
                "withdrawal-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        var reportId = reportingHandler.createReport(ReportRequestFixtures.withdrawals("exec-withdrawals-1"));

        var processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        var csvLines = readCsvLines(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(report.getFile().getFilename()).isEqualTo("withdrawals-report-" + reportId + ".csv");
        assertThat(csvLines).containsExactly(
                "created_date,created_time,finalized_date,finalized_time,withdrawal_id,status,amount,currency," +
                        "trx_id,provider_id,terminal_id,wallet_id,exchange_rate_internal,provider_amount," +
                        "provider_currency,converted_amount",
                "2026-01-01,10:00:00,2026-01-01,11:00:00,withdrawal-1,succeeded,20.00,RUB,trx-w-1," +
                        "provider-1,terminal-1,wallet-1,1.0500000000,19.90,EUR,20.00"
        );
    }

    @Test
    void failedUploadIsRetriedAndThenMarkedFailedAtAttemptLimit() throws Exception {
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-2",
                "payment-2",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("exec-failure-1"));
        stubFileStorageClient.setFailUploads(true);

        reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));
        var pendingAfterRetry = reportingHandler.getReport(new GetReportRequest(reportId));

        reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:31Z"));
        var failedReport = reportingHandler.getReport(new GetReportRequest(reportId));

        assertThat(pendingAfterRetry.getStatus()).isEqualTo(ReportStatus.pending);
        assertThat(pendingAfterRetry.getError().getCode()).isEqualTo("report_processing_error");
        assertThat(pendingAfterRetry.isSetFile()).isFalse();
        assertThat(failedReport.getStatus()).isEqualTo(ReportStatus.failed);
        assertThat(failedReport.getError().getCode()).isEqualTo("report_processing_error");
        assertThat(failedReport.isSetFinishedAt()).isTrue();
        assertThat(failedReport.isSetFile()).isFalse();
    }

    @Test
    void staleProcessingReportIsMarkedTimedOut() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("exec-timeout-1"));
        var startedAt = Instant.parse("2026-01-01T12:00:00Z");

        reportLifecycleDao.claimNextPendingReport(startedAt).orElseThrow();
        var updated = reportLifecycleService.timeoutStaleProcessingReports(Instant.parse("2026-01-01T12:01:01Z"));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));

        assertThat(updated).isEqualTo(1);
        assertThat(report.getStatus()).isEqualTo(ReportStatus.timed_out);
        assertThat(report.getError().getCode()).isEqualTo("worker_timeout");
    }

    @Test
    void createdReportExpiresAfterConfiguredTtl() throws Exception {
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-3",
                "payment-3",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("exec-expire-1"));
        var claimTime = Instant.parse("2026-01-01T12:00:00Z");

        reportLifecycleService.processNextPendingReport(claimTime);
        var expired = reportLifecycleService.expireReadyReports(claimTime.plusSeconds(601));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));

        assertThat(expired).isEqualTo(1);
        assertThat(report.getStatus()).isEqualTo(ReportStatus.expired);
        assertThat(report.getExpiresAt()).isEqualTo(claimTime.plusSeconds(600).toString());
    }

    private List<String> readCsvLines(byte[] bytes, java.nio.charset.Charset charset) {
        return new String(bytes, charset).lines().toList();
    }
}
