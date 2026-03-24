package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.dao.mapper.ReportLifecycleMapper;
import dev.vality.ccreporter.domain.enums.ReportStatus;
import dev.vality.ccreporter.model.ClaimedReportJob;
import dev.vality.ccreporter.model.ReportFileMetadata;
import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Optional;

import static dev.vality.ccreporter.dao.support.ReportDaoSupport.*;
import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;
import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;

@Repository
@RequiredArgsConstructor
public class ReportLifecycleDao {

    private static final Field<Long> CANDIDATE_ID = DSL.field(DSL.name("candidate", "id"), Long.class);
    private static final ReportStatus PENDING = ReportStatus.pending;
    private static final ReportStatus PROCESSING = ReportStatus.processing;
    private static final ReportStatus CREATED = ReportStatus.created;
    private static final ReportStatus CANCELED = ReportStatus.canceled;
    private static final ReportStatus TIMED_OUT = ReportStatus.timed_out;
    private static final ReportStatus EXPIRED = ReportStatus.expired;
    private static final String WORKER_TIMEOUT_CODE = "worker_timeout";
    private static final String WORKER_TIMEOUT_MESSAGE = "Report processing exceeded stale timeout";

    private final DSLContext dslContext;

    public Optional<ClaimedReportJob> claimNextPendingReport(Instant now) {
        var reportJob = REPORT_JOB.as("r");
        var candidate = DSL.table(DSL.name("candidate"));
        return dslContext.with("candidate").as(
                        dslContext.select(REPORT_JOB.ID)
                                .from(REPORT_JOB)
                                .where(REPORT_JOB.STATUS.eq(PENDING))
                                .and(isReadyForClaim(now))
                                .orderBy(REPORT_JOB.CREATED_AT.asc(), REPORT_JOB.ID.asc())
                                .limit(1)
                                .forUpdate()
                                .skipLocked()
                )
                .update(reportJob)
                .set(reportJob.STATUS, PROCESSING)
                .set(reportJob.ATTEMPT, reportJob.ATTEMPT.plus(1))
                .set(reportJob.STARTED_AT, firstTimestampOrExisting(now, reportJob.STARTED_AT))
                .set(reportJob.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .set(reportJob.UPDATED_AT, timestampValue(now, reportJob.UPDATED_AT))
                .from(candidate)
                .where(reportJob.ID.eq(CANDIDATE_ID))
                .returningResult(
                        reportJob.ID,
                        reportJob.REPORT_TYPE,
                        reportJob.FILE_TYPE,
                        reportJob.QUERY_JSON,
                        reportJob.TIMEZONE,
                        reportJob.ATTEMPT
                )
                .fetchOptional(ReportLifecycleMapper::mapClaimedReport);
    }

    public boolean cancelReport(String createdBy, long reportId, Instant now) {
        var updated = dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, CANCELED)
                .set(REPORT_JOB.FINISHED_AT, firstTimestampOrExisting(now, REPORT_JOB.FINISHED_AT))
                .set(REPORT_JOB.UPDATED_AT, timestampValue(now, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.CREATED_BY.eq(createdBy))
                .and(REPORT_JOB.STATUS.in(PENDING, PROCESSING))
                .execute();
        return updated > 0;
    }

    public boolean rescheduleForRetry(long reportId, Instant nextAttemptAt, String errorCode, String errorMessage) {
        var updated = dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, PENDING)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, timestampValue(nextAttemptAt, REPORT_JOB.NEXT_ATTEMPT_AT))
                .set(REPORT_JOB.ERROR_CODE, errorCode)
                .set(REPORT_JOB.ERROR_MESSAGE, errorMessage)
                .set(REPORT_JOB.UPDATED_AT, timestampValue(nextAttemptAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(PROCESSING))
                .execute();
        return updated > 0;
    }

    public boolean markCreated(
            long reportId,
            Instant dataSnapshotFixedAt,
            Instant finishedAt,
            Instant expiresAt,
            long rowsCount
    ) {
        var updated = dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, CREATED)
                .set(REPORT_JOB.DATA_SNAPSHOT_FIXED_AT,
                        firstTimestampOrExisting(dataSnapshotFixedAt, REPORT_JOB.DATA_SNAPSHOT_FIXED_AT))
                .set(REPORT_JOB.FINISHED_AT, firstTimestampOrExisting(finishedAt, REPORT_JOB.FINISHED_AT))
                .set(REPORT_JOB.EXPIRES_AT, timestampValue(expiresAt, REPORT_JOB.EXPIRES_AT))
                .set(REPORT_JOB.ROWS_COUNT, rowsCount)
                .set(REPORT_JOB.ERROR_CODE, (String) null)
                .set(REPORT_JOB.ERROR_MESSAGE, (String) null)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .set(REPORT_JOB.UPDATED_AT, timestampValue(finishedAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(PROCESSING))
                .execute();
        return updated > 0;
    }

    public boolean publishFileRecord(long reportId, ReportFileMetadata fileMetadata, Instant createdAt) {
        var updated = dslContext.insertInto(REPORT_FILE)
                .set(ReportLifecycleMapper.newInsertableFileRecord(dslContext, reportId, fileMetadata, createdAt))
                .execute();
        return updated > 0;
    }

    public boolean markFailed(long reportId, Instant dataSnapshotFixedAt, Instant finishedAt, String code,
                              String message) {
        return markTerminal(reportId, ReportStatus.failed, dataSnapshotFixedAt, finishedAt, code, message);
    }

    public boolean markTimedOut(long reportId, Instant dataSnapshotFixedAt, Instant finishedAt, String code,
                                String message) {
        return markTerminal(reportId, ReportStatus.timed_out, dataSnapshotFixedAt, finishedAt, code, message);
    }

    public boolean expireReport(long reportId, Instant expiredAt) {
        var updated = dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, EXPIRED)
                .set(REPORT_JOB.FINISHED_AT, firstTimestampOrExisting(expiredAt, REPORT_JOB.FINISHED_AT))
                .set(REPORT_JOB.UPDATED_AT, timestampValue(expiredAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(CREATED))
                .execute();
        return updated > 0;
    }

    public int timeoutStaleProcessingReports(Instant staleBefore, Instant finishedAt) {
        return dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, TIMED_OUT)
                .set(REPORT_JOB.FINISHED_AT, firstTimestampOrExisting(finishedAt, REPORT_JOB.FINISHED_AT))
                .set(REPORT_JOB.ERROR_CODE, firstValueOrExisting(WORKER_TIMEOUT_CODE, REPORT_JOB.ERROR_CODE))
                .set(REPORT_JOB.ERROR_MESSAGE, firstValueOrExisting(WORKER_TIMEOUT_MESSAGE, REPORT_JOB.ERROR_MESSAGE))
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .set(REPORT_JOB.UPDATED_AT, timestampValue(finishedAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.STATUS.eq(PROCESSING))
                .and(REPORT_JOB.UPDATED_AT.le(timestampValue(staleBefore, REPORT_JOB.UPDATED_AT)))
                .execute();
    }

    public int expireReports(Instant now) {
        return dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, EXPIRED)
                .set(
                        REPORT_JOB.FINISHED_AT,
                        DSL.coalesce(
                                REPORT_JOB.FINISHED_AT,
                                REPORT_JOB.EXPIRES_AT,
                                timestampValue(now, REPORT_JOB.FINISHED_AT)
                        )
                )
                .set(REPORT_JOB.UPDATED_AT, timestampValue(now, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.STATUS.eq(CREATED))
                .and(REPORT_JOB.EXPIRES_AT.isNotNull())
                .and(REPORT_JOB.EXPIRES_AT.le(timestampValue(now, REPORT_JOB.EXPIRES_AT)))
                .execute();
    }

    private boolean markTerminal(
            long reportId,
            ReportStatus terminalStatus,
            Instant dataSnapshotFixedAt,
            Instant finishedAt,
            String code,
            String message
    ) {
        var updated = dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, terminalStatus)
                .set(REPORT_JOB.DATA_SNAPSHOT_FIXED_AT,
                        firstTimestampOrExisting(dataSnapshotFixedAt, REPORT_JOB.DATA_SNAPSHOT_FIXED_AT))
                .set(REPORT_JOB.FINISHED_AT, firstTimestampOrExisting(finishedAt, REPORT_JOB.FINISHED_AT))
                .set(REPORT_JOB.ERROR_CODE, code)
                .set(REPORT_JOB.ERROR_MESSAGE, message)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .set(REPORT_JOB.UPDATED_AT, timestampValue(finishedAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(PROCESSING))
                .execute();
        return updated > 0;
    }

    private static Condition isReadyForClaim(Instant now) {
        return isReadyAt(REPORT_JOB.NEXT_ATTEMPT_AT, now);
    }
}
