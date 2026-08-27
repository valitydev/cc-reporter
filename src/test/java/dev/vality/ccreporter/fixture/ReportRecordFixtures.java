package dev.vality.ccreporter.fixture;

import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Подготавливает записи о заданиях и файлах отчётов для сценариев, которым нужен уже заданный status в базе.
 */
public final class ReportRecordFixtures {

    private ReportRecordFixtures() {
    }

    public static void markReportProcessing(
            JdbcTemplate jdbcTemplate,
            long reportId,
            Instant startedAt,
            Instant snapshotFixedAt
    ) {
        jdbcTemplate.update(
                """
                        UPDATE ccr.report_job
                        SET status = 'processing',
                            started_at = ?,
                            data_snapshot_fixed_at = ?
                        WHERE id = ?
                        """,
                toUtcLocalDateTime(startedAt),
                toUtcLocalDateTime(snapshotFixedAt),
                reportId
        );
    }

    public static void markReportCreated(
            JdbcTemplate jdbcTemplate,
            long reportId,
            Instant startedAt,
            Instant snapshotFixedAt,
            Instant finishedAt,
            Instant expiresAt,
            long rowsCount
    ) {
        jdbcTemplate.update(
                """
                        UPDATE ccr.report_job
                        SET status = 'created',
                            started_at = ?,
                            data_snapshot_fixed_at = ?,
                            finished_at = ?,
                            expires_at = ?,
                            rows_count = ?
                        WHERE id = ?
                        """,
                toUtcLocalDateTime(startedAt),
                toUtcLocalDateTime(snapshotFixedAt),
                toUtcLocalDateTime(finishedAt),
                toUtcLocalDateTime(expiresAt),
                rowsCount,
                reportId
        );
    }

    public static void markReportFailed(
            JdbcTemplate jdbcTemplate,
            long reportId,
            Instant startedAt,
            Instant snapshotFixedAt,
            Instant finishedAt,
            String errorCode,
            String errorMessage
    ) {
        jdbcTemplate.update(
                """
                        UPDATE ccr.report_job
                        SET status = 'failed',
                            started_at = ?,
                            data_snapshot_fixed_at = ?,
                            finished_at = ?,
                            error_code = ?,
                            error_message = ?
                        WHERE id = ?
                        """,
                toUtcLocalDateTime(startedAt),
                toUtcLocalDateTime(snapshotFixedAt),
                toUtcLocalDateTime(finishedAt),
                errorCode,
                errorMessage,
                reportId
        );
    }

    public static void attachCsvFile(
            JdbcTemplate jdbcTemplate,
            long reportId,
            String fileId,
            Instant createdAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO ccr.report_file (
                            report_id,
                            file_id,
                            file_type,
                            filename,
                            content_type,
                            size_bytes,
                            md5,
                            sha256,
                            created_at
                        )
                        VALUES (?, ?, 'csv', ?, 'text/csv', ?, ?, ?, ?)
                        """,
                reportId,
                fileId,
                "payments.csv",
                128L,
                "md5-value",
                "sha256-value",
                toUtcLocalDateTime(createdAt)
        );
    }

    private static LocalDateTime toUtcLocalDateTime(Instant value) {
        return LocalDateTime.ofInstant(value, ZoneOffset.UTC);
    }
}
