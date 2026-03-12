package dev.vality.ccreporter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.vality.ccreporter.CancelReportRequest;
import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.GetReportsFilter;
import dev.vality.ccreporter.GetReportsRequest;
import dev.vality.ccreporter.GetReportsResponse;
import dev.vality.ccreporter.Report;
import dev.vality.ccreporter.ReportStatus;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReportLifecycleIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void reportLifecycleProgressesFromPendingToCreated() throws Exception {
        long reportId = reportingHandler.createReport(createPaymentsReportRequest("lifecycle-created-1"));
        final Instant startedAt = Instant.parse("2026-01-02T10:00:00Z");
        final Instant snapshotFixedAt = Instant.parse("2026-01-02T10:05:00Z");
        final Instant finishedAt = Instant.parse("2026-01-02T10:10:00Z");
        final Instant expiresAt = Instant.parse("2026-02-01T00:00:00Z");

        Report pendingReport = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(pendingReport.getStatus()).isEqualTo(ReportStatus.pending);
        assertThat(pendingReport.isSetStartedAt()).isFalse();
        assertThat(pendingReport.isSetFinishedAt()).isFalse();
        assertThat(pendingReport.isSetFile()).isFalse();

        markReportProcessing(reportId, startedAt, snapshotFixedAt);

        Report processingReport = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(processingReport.getStatus()).isEqualTo(ReportStatus.processing);
        assertThat(processingReport.getStartedAt()).isEqualTo(startedAt.toString());
        assertThat(processingReport.getDataSnapshotFixedAt()).isEqualTo(snapshotFixedAt.toString());
        assertThat(processingReport.isSetFinishedAt()).isFalse();

        markReportCreated(reportId, startedAt, snapshotFixedAt, finishedAt, expiresAt, 42L);
        attachCsvFile(reportId, "file-lifecycle-1", finishedAt);

        Report createdReport = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(createdReport.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(createdReport.getStartedAt()).isEqualTo(startedAt.toString());
        assertThat(createdReport.getDataSnapshotFixedAt()).isEqualTo(snapshotFixedAt.toString());
        assertThat(createdReport.getFinishedAt()).isEqualTo(finishedAt.toString());
        assertThat(createdReport.getRowsCount()).isEqualTo(42L);
        assertThat(createdReport.getExpiresAt()).isEqualTo(expiresAt.toString());
        assertThat(createdReport.getFile().getFileId()).isEqualTo("file-lifecycle-1");
        assertThat(createdReport.getFile().getFilename()).isEqualTo("payments.csv");

        GetReportsFilter filter = new GetReportsFilter();
        filter.setStatuses(List.of(ReportStatus.created));
        GetReportsResponse response = reportingHandler.getReports(new GetReportsRequest().setFilter(filter));

        assertThat(response.getReports()).extracting(Report::getReportId).contains(reportId);
    }

    @Test
    void processingReportCanBeCanceledAndBecomesTerminal() throws Exception {
        long reportId = reportingHandler.createReport(createPaymentsReportRequest("cancel-processing-1"));
        Instant startedAt = Instant.parse("2026-01-03T10:00:00Z");
        Instant snapshotFixedAt = Instant.parse("2026-01-03T10:02:00Z");

        markReportProcessing(reportId, startedAt, snapshotFixedAt);

        reportingHandler.cancelReport(new CancelReportRequest(reportId));
        reportingHandler.cancelReport(new CancelReportRequest(reportId));

        Report canceledReport = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(canceledReport.getStatus()).isEqualTo(ReportStatus.canceled);
        assertThat(canceledReport.getStartedAt()).isEqualTo(startedAt.toString());
        assertThat(canceledReport.getDataSnapshotFixedAt()).isEqualTo(snapshotFixedAt.toString());
        assertThat(canceledReport.getFinishedAt()).isNotBlank();
    }

    @Test
    void failedReportRemainsFailedWhenCancelIsCalled() throws Exception {
        long reportId = reportingHandler.createReport(createPaymentsReportRequest("failed-1"));
        Instant startedAt = Instant.parse("2026-01-04T10:00:00Z");
        Instant snapshotFixedAt = Instant.parse("2026-01-04T10:03:00Z");
        Instant finishedAt = Instant.parse("2026-01-04T10:07:00Z");

        markReportFailed(reportId, startedAt, snapshotFixedAt, finishedAt, "storage_error", "upload failed");

        reportingHandler.cancelReport(new CancelReportRequest(reportId));

        Report failedReport = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(failedReport.getStatus()).isEqualTo(ReportStatus.failed);
        assertThat(failedReport.getFinishedAt()).isEqualTo(finishedAt.toString());
        assertThat(failedReport.getError().getCode()).isEqualTo("storage_error");
        assertThat(failedReport.getError().getMessage()).isEqualTo("upload failed");
        assertThat(failedReport.isSetFile()).isFalse();
    }
}
