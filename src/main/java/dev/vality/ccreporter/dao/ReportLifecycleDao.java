package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.domain.enums.ReportStatus;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.model.ReportTask;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Optional;

import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;
import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;
import static dev.vality.ccreporter.util.TimestampUtils.toLocalDateTime;

@Repository
@RequiredArgsConstructor
public class ReportLifecycleDao {

    private static final Field<Long> CANDIDATE_ID = DSL.field(DSL.name("candidate", "id"), Long.class);
    private static final String WORKER_TIMEOUT_CODE = "worker_timeout";
    private static final String WORKER_TIMEOUT_MESSAGE = "Report processing exceeded stale timeout";

    private final DSLContext dslContext;

    public Optional<ReportTask> claimNextPendingReport(Instant now) {
        var candidate = DSL.table(DSL.name("candidate"));
        var claimTime = toLocalDateTime(now);
        return dslContext.with("candidate").as(
                        dslContext.select(REPORT_JOB.ID)
                                .from(REPORT_JOB)
                                .where(REPORT_JOB.STATUS.eq(ReportStatus.pending))
                                .and(REPORT_JOB.NEXT_ATTEMPT_AT.isNull()
                                        .or(REPORT_JOB.NEXT_ATTEMPT_AT.le(claimTime)))
                                .orderBy(REPORT_JOB.CREATED_AT.asc(), REPORT_JOB.ID.asc())
                                .limit(1)
                                .forUpdate()
                                .skipLocked()
                )
                .update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, ReportStatus.processing)
                .set(REPORT_JOB.ATTEMPT, REPORT_JOB.ATTEMPT.plus(1))
                .set(REPORT_JOB.STARTED_AT, claimTime)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .from(candidate)
                .where(REPORT_JOB.ID.eq(CANDIDATE_ID))
                .returning(
                        REPORT_JOB.ID,
                        REPORT_JOB.REPORT_TYPE,
                        REPORT_JOB.QUERY_JSON,
                        REPORT_JOB.TIMEZONE,
                        REPORT_JOB.ATTEMPT
                )
                .fetchOptional(record -> new ReportTask(
                        record.get(REPORT_JOB.ID),
                        record.get(REPORT_JOB.REPORT_TYPE),
                        record.get(REPORT_JOB.QUERY_JSON).data(),
                        record.get(REPORT_JOB.TIMEZONE),
                        record.get(REPORT_JOB.ATTEMPT)
                ));
    }

    public boolean cancelReport(String createdBy, long reportId, Instant now) {
        return dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, ReportStatus.canceled)
                .set(REPORT_JOB.FINISHED_AT, toLocalDateTime(now))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.CREATED_BY.eq(createdBy))
                .and(REPORT_JOB.STATUS.in(ReportStatus.pending, ReportStatus.processing))
                .execute() == 1;
    }

    public void rescheduleForRetry(long reportId, Instant nextAttemptAt, String errorCode, String errorMessage) {
        dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, ReportStatus.pending)
                .set(REPORT_JOB.STARTED_AT, (LocalDateTime) null)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, toLocalDateTime(nextAttemptAt))
                .set(REPORT_JOB.ERROR_CODE, errorCode)
                .set(REPORT_JOB.ERROR_MESSAGE, errorMessage)
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(ReportStatus.processing))
                .execute();
    }

    public void markFailed(long reportId, Instant finishedAt, String code, String message) {
        dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, ReportStatus.failed)
                .set(REPORT_JOB.FINISHED_AT, toLocalDateTime(finishedAt))
                .set(REPORT_JOB.ERROR_CODE, code)
                .set(REPORT_JOB.ERROR_MESSAGE, message)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(ReportStatus.processing))
                .execute();
    }

    @Transactional
    public boolean completeReport(
            long reportId,
            ReportFile reportFile,
            Instant dataSnapshotFixedAt,
            Instant finishedAt,
            Instant expiresAt,
            long rowsCount
    ) {
        var completed = dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, ReportStatus.created)
                .set(REPORT_JOB.DATA_SNAPSHOT_FIXED_AT, toLocalDateTime(dataSnapshotFixedAt))
                .set(REPORT_JOB.FINISHED_AT, toLocalDateTime(finishedAt))
                .set(REPORT_JOB.EXPIRES_AT, toLocalDateTime(expiresAt))
                .set(REPORT_JOB.ROWS_COUNT, rowsCount)
                .set(REPORT_JOB.ERROR_CODE, (String) null)
                .set(REPORT_JOB.ERROR_MESSAGE, (String) null)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(ReportStatus.processing))
                .execute();
        if (completed == 0) {
            return false;
        }

        dslContext.insertInto(REPORT_FILE)
                .columns(
                        REPORT_FILE.REPORT_ID,
                        REPORT_FILE.FILE_ID,
                        REPORT_FILE.FILE_TYPE,
                        REPORT_FILE.FILENAME,
                        REPORT_FILE.CONTENT_TYPE,
                        REPORT_FILE.SIZE_BYTES,
                        REPORT_FILE.MD5,
                        REPORT_FILE.SHA256,
                        REPORT_FILE.CREATED_AT
                )
                .values(
                        reportId,
                        reportFile.getFileId(),
                        reportFile.getFileType(),
                        reportFile.getFilename(),
                        reportFile.getContentType(),
                        reportFile.getSizeBytes(),
                        reportFile.getMd5(),
                        reportFile.getSha256(),
                        toLocalDateTime(finishedAt)
                )
                .execute();
        return true;
    }

    public int timeoutStaleProcessingReports(Instant staleBefore, Instant finishedAt) {
        return dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, ReportStatus.timed_out)
                .set(REPORT_JOB.FINISHED_AT, toLocalDateTime(finishedAt))
                .set(REPORT_JOB.ERROR_CODE, WORKER_TIMEOUT_CODE)
                .set(REPORT_JOB.ERROR_MESSAGE, WORKER_TIMEOUT_MESSAGE)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .where(REPORT_JOB.STATUS.eq(ReportStatus.processing))
                .and(REPORT_JOB.STARTED_AT.le(toLocalDateTime(staleBefore)))
                .execute();
    }

    public int expireReports(Instant now) {
        return dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, ReportStatus.expired)
                .where(REPORT_JOB.STATUS.eq(ReportStatus.created))
                .and(REPORT_JOB.EXPIRES_AT.le(toLocalDateTime(now)))
                .execute();
    }
}
