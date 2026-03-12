package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.service.ReportFileMetadata;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ReportLifecycleDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public ReportLifecycleDao(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<ClaimedReportJob> claimNextPendingReport(Instant now) {
        List<ClaimedReportJob> claimedReports = jdbcTemplate.query(
                """
                WITH candidate AS (
                    SELECT id
                    FROM ccr.report_job
                    WHERE status = CAST(:pendingStatus AS ccr.report_status)
                      AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
                    ORDER BY created_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE ccr.report_job r
                SET status = CAST(:processingStatus AS ccr.report_status),
                    attempt = r.attempt + 1,
                    started_at = COALESCE(r.started_at, :now),
                    next_attempt_at = NULL,
                    updated_at = :now
                FROM candidate
                WHERE r.id = candidate.id
                RETURNING r.id, r.report_type, r.file_type, r.query_json, r.timezone, r.attempt
                """,
                new MapSqlParameterSource()
                        .addValue("pendingStatus", ReportStatus.pending.name())
                        .addValue("processingStatus", ReportStatus.processing.name())
                        .addValue("now", Timestamp.from(now)),
                ReportLifecycleDao::mapClaimedReport
        );
        return claimedReports.stream().findFirst();
    }

    public boolean rescheduleForRetry(long reportId, Instant nextAttemptAt, String errorCode, String errorMessage) {
        int updated = jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = CAST(:pendingStatus AS ccr.report_status),
                    next_attempt_at = :nextAttemptAt,
                    error_code = :errorCode,
                    error_message = :errorMessage,
                    updated_at = :updatedAt
                WHERE id = :reportId
                  AND status = CAST(:processingStatus AS ccr.report_status)
                """,
                new MapSqlParameterSource()
                        .addValue("pendingStatus", ReportStatus.pending.name())
                        .addValue("processingStatus", ReportStatus.processing.name())
                        .addValue("nextAttemptAt", Timestamp.from(nextAttemptAt))
                        .addValue("errorCode", errorCode)
                        .addValue("errorMessage", errorMessage)
                        .addValue("updatedAt", Timestamp.from(nextAttemptAt))
                        .addValue("reportId", reportId)
        );
        return updated > 0;
    }

    public boolean markCreated(
            long reportId,
            Instant dataSnapshotFixedAt,
            Instant finishedAt,
            Instant expiresAt,
            long rowsCount
    ) {
        int updated = jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = CAST(:createdStatus AS ccr.report_status),
                    data_snapshot_fixed_at = COALESCE(data_snapshot_fixed_at, :dataSnapshotFixedAt),
                    finished_at = COALESCE(finished_at, :finishedAt),
                    expires_at = :expiresAt,
                    rows_count = :rowsCount,
                    error_code = NULL,
                    error_message = NULL,
                    next_attempt_at = NULL,
                    updated_at = :updatedAt
                WHERE id = :reportId
                  AND status = CAST(:processingStatus AS ccr.report_status)
                """,
                new MapSqlParameterSource()
                        .addValue("createdStatus", ReportStatus.created.name())
                        .addValue("processingStatus", ReportStatus.processing.name())
                        .addValue("dataSnapshotFixedAt", Timestamp.from(dataSnapshotFixedAt))
                        .addValue("finishedAt", Timestamp.from(finishedAt))
                        .addValue("expiresAt", Timestamp.from(expiresAt))
                        .addValue("rowsCount", rowsCount)
                        .addValue("updatedAt", Timestamp.from(finishedAt))
                        .addValue("reportId", reportId)
        );
        return updated > 0;
    }

    public boolean publishFileRecord(long reportId, ReportFileMetadata fileMetadata, Instant createdAt) {
        int updated = jdbcTemplate.update(
                """
                INSERT INTO ccr.report_file (
                    report_id,
                    file_id,
                    file_type,
                    bucket,
                    object_key,
                    filename,
                    content_type,
                    size_bytes,
                    md5,
                    sha256,
                    created_at
                )
                VALUES (
                    :reportId,
                    :fileId,
                    CAST(:fileType AS ccr.file_type),
                    :bucket,
                    :objectKey,
                    :fileName,
                    :contentType,
                    :sizeBytes,
                    :md5,
                    :sha256,
                    :createdAt
                )
                """,
                new MapSqlParameterSource()
                        .addValue("reportId", reportId)
                        .addValue("fileId", fileMetadata.fileId())
                        .addValue("fileType", "csv")
                        .addValue("bucket", fileMetadata.bucket())
                        .addValue("objectKey", fileMetadata.objectKey())
                        .addValue("fileName", fileMetadata.fileName())
                        .addValue("contentType", fileMetadata.contentType())
                        .addValue("sizeBytes", fileMetadata.sizeBytes())
                        .addValue("md5", fileMetadata.md5())
                        .addValue("sha256", fileMetadata.sha256())
                        .addValue("createdAt", Timestamp.from(createdAt))
        );
        return updated > 0;
    }

    public boolean markFailed(long reportId, Instant dataSnapshotFixedAt, Instant finishedAt, String code, String message) {
        return markTerminal(reportId, ReportStatus.failed, dataSnapshotFixedAt, finishedAt, code, message);
    }

    public boolean markTimedOut(long reportId, Instant dataSnapshotFixedAt, Instant finishedAt, String code, String message) {
        return markTerminal(reportId, ReportStatus.timed_out, dataSnapshotFixedAt, finishedAt, code, message);
    }

    public boolean expireReport(long reportId, Instant expiredAt) {
        int updated = jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = CAST(:expiredStatus AS ccr.report_status),
                    finished_at = COALESCE(finished_at, :expiredAt),
                    updated_at = :expiredAt
                WHERE id = :reportId
                  AND status = CAST(:createdStatus AS ccr.report_status)
                """,
                new MapSqlParameterSource()
                        .addValue("expiredStatus", ReportStatus.expired.name())
                        .addValue("createdStatus", ReportStatus.created.name())
                        .addValue("expiredAt", Timestamp.from(expiredAt))
                        .addValue("reportId", reportId)
        );
        return updated > 0;
    }

    public int timeoutStaleProcessingReports(Instant staleBefore, Instant finishedAt) {
        return jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = CAST(:timedOutStatus AS ccr.report_status),
                    finished_at = COALESCE(finished_at, :finishedAt),
                    error_code = COALESCE(error_code, :errorCode),
                    error_message = COALESCE(error_message, :errorMessage),
                    next_attempt_at = NULL,
                    updated_at = :finishedAt
                WHERE status = CAST(:processingStatus AS ccr.report_status)
                  AND updated_at <= :staleBefore
                """,
                new MapSqlParameterSource()
                        .addValue("timedOutStatus", ReportStatus.timed_out.name())
                        .addValue("processingStatus", ReportStatus.processing.name())
                        .addValue("finishedAt", Timestamp.from(finishedAt))
                        .addValue("staleBefore", Timestamp.from(staleBefore))
                        .addValue("errorCode", "worker_timeout")
                        .addValue("errorMessage", "Report processing exceeded stale timeout")
        );
    }

    public int expireReports(Instant now) {
        return jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = CAST(:expiredStatus AS ccr.report_status),
                    finished_at = COALESCE(finished_at, expires_at, :now),
                    updated_at = :now
                WHERE status = CAST(:createdStatus AS ccr.report_status)
                  AND expires_at IS NOT NULL
                  AND expires_at <= :now
                """,
                new MapSqlParameterSource()
                        .addValue("expiredStatus", ReportStatus.expired.name())
                        .addValue("createdStatus", ReportStatus.created.name())
                        .addValue("now", Timestamp.from(now))
        );
    }

    private boolean markTerminal(
            long reportId,
            ReportStatus terminalStatus,
            Instant dataSnapshotFixedAt,
            Instant finishedAt,
            String code,
            String message
    ) {
        int updated = jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = CAST(:terminalStatus AS ccr.report_status),
                    data_snapshot_fixed_at = COALESCE(data_snapshot_fixed_at, :dataSnapshotFixedAt),
                    finished_at = COALESCE(finished_at, :finishedAt),
                    error_code = :errorCode,
                    error_message = :errorMessage,
                    next_attempt_at = NULL,
                    updated_at = :updatedAt
                WHERE id = :reportId
                  AND status = CAST(:processingStatus AS ccr.report_status)
                """,
                new MapSqlParameterSource()
                        .addValue("terminalStatus", terminalStatus.name())
                        .addValue("processingStatus", ReportStatus.processing.name())
                        .addValue("dataSnapshotFixedAt", Timestamp.from(dataSnapshotFixedAt))
                        .addValue("finishedAt", Timestamp.from(finishedAt))
                        .addValue("errorCode", code)
                        .addValue("errorMessage", message)
                        .addValue("updatedAt", Timestamp.from(finishedAt))
                        .addValue("reportId", reportId)
        );
        return updated > 0;
    }

    private static ClaimedReportJob mapClaimedReport(ResultSet rs, int rowNum) throws SQLException {
        return new ClaimedReportJob(
                rs.getLong("id"),
                dev.vality.ccreporter.ReportType.valueOf(rs.getString("report_type")),
                dev.vality.ccreporter.FileType.valueOf(rs.getString("file_type")),
                readJson(rs.getObject("query_json")),
                rs.getString("timezone"),
                rs.getInt("attempt")
        );
    }

    private static String readJson(Object value) throws SQLException {
        if (value instanceof PGobject pgObject) {
            return pgObject.getValue();
        }
        if (value instanceof String string) {
            return string;
        }
        throw new SQLException("Unsupported query_json type: " + value);
    }
}
