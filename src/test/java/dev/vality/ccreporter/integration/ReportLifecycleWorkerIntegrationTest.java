package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.fixture.ReportRecordFixtures;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;

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
    void rescheduleMakesReportClaimableAgainWhenRetryTimeArrives() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("retry-1"));
        var firstClaimTime = Instant.parse("2026-01-06T11:00:00Z");
        var retryAt = Instant.parse("2026-01-06T11:05:00Z");
        var secondClaimTime = Instant.parse("2026-01-06T11:06:00Z");

        var firstClaim = reportLifecycleDao.claimNextPendingReport(firstClaimTime).orElseThrow();
        var rescheduled = reportLifecycleDao.rescheduleForRetry(
                reportId,
                retryAt,
                "storage_unavailable",
                "temporary upload issue"
        );
        var prematureClaim =
                reportLifecycleDao.claimNextPendingReport(firstClaimTime.plusSeconds(30));
        var secondClaim = reportLifecycleDao.claimNextPendingReport(secondClaimTime).orElseThrow();

        assertThat(firstClaim.id()).isEqualTo(reportId);
        assertThat(rescheduled).isTrue();
        assertThat(prematureClaim).isEmpty();
        assertThat(secondClaim.id()).isEqualTo(reportId);
        assertThat(secondClaim.attempt()).isEqualTo(2);

        var retriedReport = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(retriedReport.getStatus()).isEqualTo(ReportStatus.processing);
        assertThat(retriedReport.getError().getCode()).isEqualTo("storage_unavailable");
        assertThat(retriedReport.getError().getMessage()).isEqualTo("temporary upload issue");
    }

    @Test
    void terminalTransitionPreservesFirstFinishedAtAndBlocksLaterTerminalRewrite() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("terminal-1"));
        var claimTime = Instant.parse("2026-01-06T12:00:00Z");
        var snapshotFixedAt = Instant.parse("2026-01-06T12:01:00Z");
        var failedAt = Instant.parse("2026-01-06T12:02:00Z");
        var timedOutAt = Instant.parse("2026-01-06T12:03:00Z");

        reportLifecycleDao.claimNextPendingReport(claimTime).orElseThrow();
        var failed = reportLifecycleDao.markFailed(
                reportId,
                snapshotFixedAt,
                failedAt,
                "storage_error",
                "upload failed"
        );
        var timedOut = reportLifecycleDao.markTimedOut(
                reportId,
                snapshotFixedAt.plusSeconds(30),
                timedOutAt,
                "worker_timeout",
                "worker exceeded timeout"
        );

        assertThat(failed).isTrue();
        assertThat(timedOut).isFalse();

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.failed);
        assertThat(report.getFinishedAt()).isEqualTo(failedAt.toString());
        assertThat(report.getDataSnapshotFixedAt()).isEqualTo(snapshotFixedAt.toString());
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

        reportLifecycleDao.claimNextPendingReport(claimTime).orElseThrow();
        var created = reportLifecycleDao.markCreated(reportId, snapshotFixedAt, createdAt, expiresAt, 7L);
        ReportRecordFixtures.attachCsvFile(jdbcTemplate, reportId, "file-expire-1", createdAt);
        var expired = reportLifecycleDao.expireReport(reportId, expiredAt);

        assertThat(created).isTrue();
        assertThat(expired).isTrue();

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
        var timestamp = jdbcTemplate.queryForObject(sql, Timestamp.class, reportId);
        return Objects.requireNonNull(timestamp).toInstant();
    }

    private Instant readNullableInstant(String sql, long reportId) {
        var timestamp = jdbcTemplate.queryForObject(sql, Timestamp.class, reportId);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
