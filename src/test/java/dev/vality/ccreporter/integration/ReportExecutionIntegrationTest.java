package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.Report;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.CurrentStateTableFixtures;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

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
        long reportId = reportingHandler.createReport(ReportRequestFixtures.payments("exec-payments-1"));

        boolean processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        String csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(report.getFile().getFilename()).isEqualTo("payments-report-" + reportId + ".csv");
        assertThat(report.getFile().getContentType()).isEqualTo("text/csv");
        assertThat(report.getDataSnapshotFixedAt()).isNotBlank();
        assertThat(csv).contains("invoice_id,payment_id");
        assertThat(csv).contains("invoice-1,payment-1");
    }

    @Test
    void withdrawalsReportIsBuiltAndPublishedEndToEnd() throws Exception {
        CurrentStateTableFixtures.insertWithdrawalRow(
                jdbcTemplate,
                "withdrawal-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        long reportId = reportingHandler.createReport(ReportRequestFixtures.withdrawals("exec-withdrawals-1"));

        boolean processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        String csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(report.getFile().getFilename()).isEqualTo("withdrawals-report-" + reportId + ".csv");
        assertThat(csv).contains("withdrawal_id,party_id");
        assertThat(csv).contains("withdrawal-1,party-1");
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
        long reportId = reportingHandler.createReport(ReportRequestFixtures.payments("exec-failure-1"));
        stubFileStorageClient.setFailUploads(true);

        reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));
        Report pendingAfterRetry = reportingHandler.getReport(new GetReportRequest(reportId));

        reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:31Z"));
        Report failedReport = reportingHandler.getReport(new GetReportRequest(reportId));

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
        long reportId = reportingHandler.createReport(ReportRequestFixtures.payments("exec-timeout-1"));
        Instant startedAt = Instant.parse("2026-01-01T12:00:00Z");

        reportLifecycleDao.claimNextPendingReport(startedAt).orElseThrow();
        int updated = reportLifecycleService.timeoutStaleProcessingReports(Instant.parse("2026-01-01T12:01:01Z"));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));

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
        long reportId = reportingHandler.createReport(ReportRequestFixtures.payments("exec-expire-1"));
        Instant claimTime = Instant.parse("2026-01-01T12:00:00Z");

        reportLifecycleService.processNextPendingReport(claimTime);
        int expired = reportLifecycleService.expireReadyReports(claimTime.plusSeconds(601));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));

        assertThat(expired).isEqualTo(1);
        assertThat(report.getStatus()).isEqualTo(ReportStatus.expired);
        assertThat(report.getExpiresAt()).isEqualTo(claimTime.plusSeconds(600).toString());
    }
}
