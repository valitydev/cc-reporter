package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.model.ClaimedReportJob;
import dev.vality.ccreporter.model.ReportFileMetadata;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record6;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Optional;

import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;
import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;

@Repository
@RequiredArgsConstructor
public class ReportLifecycleDao {

    private static final Field<Long> CANDIDATE_ID = DSL.field(DSL.name("candidate", "id"), Long.class);
    private static final dev.vality.ccreporter.domain.enums.FileType CSV_FILE_TYPE =
            dev.vality.ccreporter.domain.enums.FileType.csv;

    private final DSLContext dslContext;

    public Optional<ClaimedReportJob> claimNextPendingReport(Instant now) {
        var reportJob = REPORT_JOB.as("r");
        var candidate = DSL.table(DSL.name("candidate"));
        return dslContext.with("candidate").as(
                        dslContext.select(REPORT_JOB.ID)
                                .from(REPORT_JOB)
                                .where(REPORT_JOB.STATUS.eq(toJooqStatus(ReportStatus.pending)))
                                .and(
                                        REPORT_JOB.NEXT_ATTEMPT_AT.isNull()
                                                .or(
                                                        REPORT_JOB.NEXT_ATTEMPT_AT.le(
                                                                timestampValue(now, REPORT_JOB.NEXT_ATTEMPT_AT)
                                                        )
                                                )
                                )
                                .orderBy(REPORT_JOB.CREATED_AT.asc(), REPORT_JOB.ID.asc())
                                .limit(1)
                                .forUpdate()
                                .skipLocked()
                )
                .update(reportJob)
                .set(reportJob.STATUS, toJooqStatus(ReportStatus.processing))
                .set(reportJob.ATTEMPT, reportJob.ATTEMPT.plus(1))
                .set(
                        reportJob.STARTED_AT,
                        DSL.coalesce(reportJob.STARTED_AT, timestampValue(now, reportJob.STARTED_AT))
                )
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
                .fetchOptional(ReportLifecycleDao::mapClaimedReport);
    }

    public boolean rescheduleForRetry(long reportId, Instant nextAttemptAt, String errorCode, String errorMessage) {
        var updated = dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, toJooqStatus(ReportStatus.pending))
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, timestampValue(nextAttemptAt, REPORT_JOB.NEXT_ATTEMPT_AT))
                .set(REPORT_JOB.ERROR_CODE, errorCode)
                .set(REPORT_JOB.ERROR_MESSAGE, errorMessage)
                .set(REPORT_JOB.UPDATED_AT, timestampValue(nextAttemptAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(toJooqStatus(ReportStatus.processing)))
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
                .set(REPORT_JOB.STATUS, toJooqStatus(ReportStatus.created))
                .set(
                        REPORT_JOB.DATA_SNAPSHOT_FIXED_AT,
                        DSL.coalesce(
                                REPORT_JOB.DATA_SNAPSHOT_FIXED_AT,
                                timestampValue(dataSnapshotFixedAt, REPORT_JOB.DATA_SNAPSHOT_FIXED_AT)
                        )
                )
                .set(
                        REPORT_JOB.FINISHED_AT,
                        DSL.coalesce(REPORT_JOB.FINISHED_AT, timestampValue(finishedAt, REPORT_JOB.FINISHED_AT))
                )
                .set(REPORT_JOB.EXPIRES_AT, timestampValue(expiresAt, REPORT_JOB.EXPIRES_AT))
                .set(REPORT_JOB.ROWS_COUNT, rowsCount)
                .set(REPORT_JOB.ERROR_CODE, (String) null)
                .set(REPORT_JOB.ERROR_MESSAGE, (String) null)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .set(REPORT_JOB.UPDATED_AT, timestampValue(finishedAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(toJooqStatus(ReportStatus.processing)))
                .execute();
        return updated > 0;
    }

    public boolean publishFileRecord(long reportId, ReportFileMetadata fileMetadata, Instant createdAt) {
        var reportFile = new ReportFile()
                .setReportId(reportId)
                .setFileId(fileMetadata.fileId())
                .setFileType(CSV_FILE_TYPE)
                .setBucket(fileMetadata.bucket())
                .setObjectKey(fileMetadata.objectKey())
                .setFilename(fileMetadata.fileName())
                .setContentType(fileMetadata.contentType())
                .setSizeBytes(fileMetadata.sizeBytes())
                .setMd5(fileMetadata.md5())
                .setSha256(fileMetadata.sha256())
                .setCreatedAt(LocalDateTime.ofInstant(createdAt, java.time.ZoneOffset.UTC));
        var record = dslContext.newRecord(REPORT_FILE, reportFile);
        record.changed(REPORT_FILE.ID, false);
        var updated = dslContext.insertInto(REPORT_FILE)
                .set(record)
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
                .set(REPORT_JOB.STATUS, toJooqStatus(ReportStatus.expired))
                .set(
                        REPORT_JOB.FINISHED_AT,
                        DSL.coalesce(REPORT_JOB.FINISHED_AT, timestampValue(expiredAt, REPORT_JOB.FINISHED_AT))
                )
                .set(REPORT_JOB.UPDATED_AT, timestampValue(expiredAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(toJooqStatus(ReportStatus.created)))
                .execute();
        return updated > 0;
    }

    public int timeoutStaleProcessingReports(Instant staleBefore, Instant finishedAt) {
        return dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, toJooqStatus(ReportStatus.timed_out))
                .set(
                        REPORT_JOB.FINISHED_AT,
                        DSL.coalesce(REPORT_JOB.FINISHED_AT, timestampValue(finishedAt, REPORT_JOB.FINISHED_AT))
                )
                .set(
                        REPORT_JOB.ERROR_CODE,
                        DSL.coalesce(REPORT_JOB.ERROR_CODE, DSL.val("worker_timeout", REPORT_JOB.ERROR_CODE))
                )
                .set(
                        REPORT_JOB.ERROR_MESSAGE,
                        DSL.coalesce(
                                REPORT_JOB.ERROR_MESSAGE,
                                DSL.val("Report processing exceeded stale timeout", REPORT_JOB.ERROR_MESSAGE)
                        )
                )
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .set(REPORT_JOB.UPDATED_AT, timestampValue(finishedAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.STATUS.eq(toJooqStatus(ReportStatus.processing)))
                .and(REPORT_JOB.UPDATED_AT.le(timestampValue(staleBefore, REPORT_JOB.UPDATED_AT)))
                .execute();
    }

    public int expireReports(Instant now) {
        return dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, toJooqStatus(ReportStatus.expired))
                .set(
                        REPORT_JOB.FINISHED_AT,
                        DSL.coalesce(
                                REPORT_JOB.FINISHED_AT,
                                REPORT_JOB.EXPIRES_AT,
                                timestampValue(now, REPORT_JOB.FINISHED_AT)
                        )
                )
                .set(REPORT_JOB.UPDATED_AT, timestampValue(now, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.STATUS.eq(toJooqStatus(ReportStatus.created)))
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
                .set(REPORT_JOB.STATUS, toJooqStatus(terminalStatus))
                .set(
                        REPORT_JOB.DATA_SNAPSHOT_FIXED_AT,
                        DSL.coalesce(
                                REPORT_JOB.DATA_SNAPSHOT_FIXED_AT,
                                timestampValue(dataSnapshotFixedAt, REPORT_JOB.DATA_SNAPSHOT_FIXED_AT)
                        )
                )
                .set(
                        REPORT_JOB.FINISHED_AT,
                        DSL.coalesce(REPORT_JOB.FINISHED_AT, timestampValue(finishedAt, REPORT_JOB.FINISHED_AT))
                )
                .set(REPORT_JOB.ERROR_CODE, code)
                .set(REPORT_JOB.ERROR_MESSAGE, message)
                .set(REPORT_JOB.NEXT_ATTEMPT_AT, (LocalDateTime) null)
                .set(REPORT_JOB.UPDATED_AT, timestampValue(finishedAt, REPORT_JOB.UPDATED_AT))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.STATUS.eq(toJooqStatus(ReportStatus.processing)))
                .execute();
        return updated > 0;
    }

    private static ClaimedReportJob mapClaimedReport(Record6<Long,
            dev.vality.ccreporter.domain.enums.ReportType,
            dev.vality.ccreporter.domain.enums.FileType,
            org.jooq.JSONB,
            String,
            Integer> record) {
        return new ClaimedReportJob(
                record.value1(),
                dev.vality.ccreporter.ReportType.valueOf(record.value2().getLiteral()),
                dev.vality.ccreporter.FileType.valueOf(record.value3().getLiteral()),
                record.value4().data(),
                record.value5(),
                record.value6()
        );
    }

    private static Field<LocalDateTime> timestampValue(Instant value, Field<LocalDateTime> field) {
        if (value == null) {
            return DSL.castNull(field.getDataType());
        }
        return DSL.val(Timestamp.from(value)).cast(field.getDataType());
    }

    private static dev.vality.ccreporter.domain.enums.ReportStatus toJooqStatus(ReportStatus status) {
        return dev.vality.ccreporter.domain.enums.ReportStatus.valueOf(status.name());
    }
}
