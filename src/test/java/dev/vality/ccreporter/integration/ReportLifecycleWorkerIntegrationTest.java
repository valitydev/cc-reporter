package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет работу фонового воркера, который подбирает pending-отчёты и доводит их до финального состояния.
 */
class ReportLifecycleWorkerIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void claimPicksOldestDuePendingReportAndMarksItProcessing() throws Exception {
        var firstReportId = reportingHandler.createReport(ReportRequestFixtures.payments("claim-order-1"));
        final var secondReportId = reportingHandler.createReport(ReportRequestFixtures.payments("claim-order-2"));
        var claimTime = Instant.parse("2026-01-06T10:00:00Z");

        var claimedReport = reportLifecycleDao.claimNextPendingReport(claimTime);

        assertThat(claimedReport).isPresent();
        assertThat(claimedReport.get().id()).isEqualTo(firstReportId);
        assertThat(claimedReport.get().attempt()).isEqualTo(1);
        assertReportStatus(firstReportId, ReportStatus.processing);
        assertThat(readInstant("SELECT started_at FROM ccr.report_job WHERE id = ?", firstReportId)).isEqualTo(
                claimTime);
        assertThat(
                readNullableInstant("SELECT next_attempt_at FROM ccr.report_job WHERE id = ?", firstReportId)).isNull();
        assertReportStatus(secondReportId, ReportStatus.pending);
    }

    @Test
    void lifecycleTickDrainsAllReadyReports() throws Exception {
        var firstReportId = reportingHandler.createReport(ReportRequestFixtures.payments("tick-drain-1"));
        var secondReportId = reportingHandler.createReport(ReportRequestFixtures.payments("tick-drain-2"));

        reportLifecycleService.runLifecycleTick();

        assertThat(reportingHandler.getReport(new GetReportRequest(firstReportId)).getStatus())
                .isEqualTo(ReportStatus.created);
        assertThat(reportingHandler.getReport(new GetReportRequest(secondReportId)).getStatus())
                .isEqualTo(ReportStatus.created);
    }

    @Test
    void rescheduleMakesReportClaimableAgainWhenRetryTimeArrives() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("retry-1"));
        var firstClaimTime = Instant.parse("2026-01-06T11:00:00Z");
        var retryAt = Instant.parse("2026-01-06T11:05:00Z");
        var secondClaimTime = Instant.parse("2026-01-06T11:06:00Z");

        var firstClaim = reportLifecycleDao.claimNextPendingReport(firstClaimTime).orElseThrow();
        reportLifecycleDao.rescheduleForRetry(
                reportId,
                retryAt,
                "storage_unavailable",
                "temporary upload issue"
        );
        var prematureClaim = reportLifecycleDao.claimNextPendingReport(firstClaimTime.plusSeconds(30));
        var secondClaim = reportLifecycleDao.claimNextPendingReport(secondClaimTime).orElseThrow();

        assertThat(firstClaim.id()).isEqualTo(reportId);
        assertThat(prematureClaim).isEmpty();
        assertThat(secondClaim.id()).isEqualTo(reportId);
        assertThat(secondClaim.attempt()).isEqualTo(2);

        var retriedReport = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(retriedReport.getStatus()).isEqualTo(ReportStatus.processing);
        assertThat(retriedReport.getError().getCode()).isEqualTo("storage_unavailable");
        assertThat(retriedReport.getError().getMessage()).isEqualTo("temporary upload issue");
        assertThat(retriedReport.getStartedAt()).isEqualTo(secondClaimTime.toString());
    }

    @Test
    void terminalTransitionBlocksLaterTimeoutRewrite() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("terminal-1"));
        var claimTime = Instant.parse("2026-01-06T12:00:00Z");
        var failedAt = Instant.parse("2026-01-06T12:02:00Z");
        var timedOutAt = Instant.parse("2026-01-06T12:03:00Z");

        reportLifecycleDao.claimNextPendingReport(claimTime).orElseThrow();
        reportLifecycleDao.markFailed(reportId, failedAt, "storage_error", "upload failed");
        var timedOut = reportLifecycleDao.timeoutStaleProcessingReports(timedOutAt, timedOutAt);

        assertThat(timedOut).isZero();

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.failed);
        assertThat(report.getFinishedAt()).isEqualTo(failedAt.toString());
        assertThat(report.isSetDataSnapshotFixedAt()).isFalse();
        assertThat(report.getError().getCode()).isEqualTo("storage_error");
        assertThat(report.getError().getMessage()).isEqualTo("upload failed");
    }

    @Test
    void createdReportCanBeExpiredWithoutChangingFinishedAt() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("expire-1"));
        var claimTime = Instant.parse("2026-01-06T13:00:00Z");
        var snapshotFixedAt = Instant.parse("2026-01-06T13:01:00Z");
        var createdAt = Instant.parse("2026-01-06T13:02:00Z");
        var expiresAt = Instant.parse("2026-02-06T00:00:00Z");
        var expiredAt = Instant.parse("2026-02-06T00:05:00Z");
        var reportFile = new ReportFile()
                .setFileId("file-expire-1")
                .setFileType(dev.vality.ccreporter.domain.enums.FileType.csv)
                .setFilename("payments.csv")
                .setContentType("text/csv")
                .setSizeBytes(128L)
                .setMd5("md5-value")
                .setSha256("sha256-value");

        reportLifecycleDao.claimNextPendingReport(claimTime).orElseThrow();
        var created = reportLifecycleDao.completeReport(
                reportId,
                reportFile,
                snapshotFixedAt,
                createdAt,
                expiresAt,
                7L
        );
        var expired = reportLifecycleDao.expireReports(expiredAt);

        assertThat(created).isTrue();
        assertThat(expired).isEqualTo(1);

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.expired);
        assertThat(report.getFinishedAt()).isEqualTo(createdAt.toString());
        assertThat(report.getExpiresAt()).isEqualTo(expiresAt.toString());
        assertThat(report.getRowsCount()).isEqualTo(7L);
        assertThat(report.getFile().getFileId()).isEqualTo("file-expire-1");
    }

    private void assertReportStatus(long reportId, ReportStatus expectedStatus) {
        var status = jdbcTemplate.queryForObject(
                "SELECT status::text FROM ccr.report_job WHERE id = ?",
                String.class,
                reportId
        );
        assertThat(status).isEqualTo(expectedStatus.name());
    }

    private Instant readInstant(String sql, long reportId) {
        var timestamp = jdbcTemplate.queryForObject(sql, LocalDateTime.class, reportId);
        return timestamp.toInstant(ZoneOffset.UTC);
    }

    private Instant readNullableInstant(String sql, long reportId) {
        var timestamp = jdbcTemplate.queryForObject(sql, LocalDateTime.class, reportId);
        return timestamp == null ? null : timestamp.toInstant(ZoneOffset.UTC);
    }
}
