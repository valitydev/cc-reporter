package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.domain.tables.pojos.ReportJob;
import dev.vality.ccreporter.model.StoredReport;
import dev.vality.ccreporter.model.StoredReportFile;
import dev.vality.ccreporter.util.ContinuationTokenCodec.PageCursor;
import dev.vality.ccreporter.util.ThriftQueryCodec;
import dev.vality.ccreporter.util.TimestampUtils;
import org.jooq.*;
import org.jooq.Record;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;
import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;

@Repository
public class ReportDao {

    private final DSLContext dslContext;
    private final ThriftQueryCodec thriftQueryCodec;

    public ReportDao(DSLContext dslContext, ThriftQueryCodec thriftQueryCodec) {
        this.dslContext = dslContext;
        this.thriftQueryCodec = thriftQueryCodec;
    }

    public Optional<Long> findByIdempotencyKey(String createdBy, String idempotencyKey) {
        if (!StringUtils.hasText(idempotencyKey)) {
            return Optional.empty();
        }
        return dslContext.select(REPORT_JOB.ID)
                .from(REPORT_JOB)
                .where(REPORT_JOB.CREATED_BY.eq(createdBy))
                .and(REPORT_JOB.IDEMPOTENCY_KEY.eq(idempotencyKey))
                .fetchOptional(REPORT_JOB.ID);
    }

    public long createReport(
            String createdBy,
            ReportType reportType,
            FileType fileType,
            ReportQuery query,
            String timezone,
            String idempotencyKey
    ) {
        var timeRange = thriftQueryCodec.extractTimeRange(query);
        var queryJson = thriftQueryCodec.serialize(query);
        var queryHash = thriftQueryCodec.hash(queryJson);
        var reportJob = new ReportJob();
        reportJob.setReportType(toJooqReportType(reportType));
        reportJob.setFileType(toJooqFileType(fileType));
        reportJob.setQueryJson(JSONB.jsonb(queryJson));
        reportJob.setQueryHash(queryHash);
        reportJob.setRequestedTimeFrom(toLocalDateTime(timeRange.from()));
        reportJob.setRequestedTimeTo(toLocalDateTime(timeRange.to()));
        reportJob.setTimezone(timezone);
        reportJob.setCreatedBy(createdBy);
        reportJob.setIdempotencyKey(StringUtils.hasText(idempotencyKey) ? idempotencyKey : null);

        try {
            var record = dslContext.newRecord(REPORT_JOB, reportJob);
            record.changed(REPORT_JOB.ID, false);
            record.changed(REPORT_JOB.STATUS, false);
            record.changed(REPORT_JOB.ATTEMPT, false);
            record.changed(REPORT_JOB.CREATED_AT, false);
            record.changed(REPORT_JOB.UPDATED_AT, false);
            return Objects.requireNonNull(
                    dslContext.insertInto(REPORT_JOB)
                            .set(record)
                            .returningResult(REPORT_JOB.ID)
                            .fetchOne(REPORT_JOB.ID),
                    "Report creation must return an id"
            );
        } catch (org.jooq.exception.IntegrityConstraintViolationException ex) {
            throw new DuplicateKeyException("Report idempotency key already exists", ex);
        }
    }

    public Optional<StoredReport> getReport(String createdBy, long reportId) {
        return baseReportSelect()
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.CREATED_BY.eq(createdBy))
                .fetchOptional(ReportDao::mapReport);
    }

    public List<StoredReport> getReports(
            String createdBy,
            GetReportsFilter filter,
            PageCursor cursor,
            int limit
    ) {
        var conditions = new ArrayList<Condition>();
        conditions.add(REPORT_JOB.CREATED_BY.eq(createdBy));

        if (filter != null && filter.isSetStatuses() && !filter.getStatuses().isEmpty()) {
            conditions.add(
                    REPORT_JOB.STATUS.in(filter.getStatuses().stream().map(ReportDao::toJooqReportStatus).toList())
            );
        }
        if (filter != null && filter.isSetReportTypes() && !filter.getReportTypes().isEmpty()) {
            conditions.add(
                    REPORT_JOB.REPORT_TYPE.in(
                            filter.getReportTypes().stream().map(ReportDao::toJooqReportType).toList()
                    )
            );
        }
        if (filter != null && filter.isSetFileTypes() && !filter.getFileTypes().isEmpty()) {
            conditions.add(
                    REPORT_JOB.FILE_TYPE.in(filter.getFileTypes().stream().map(ReportDao::toJooqFileType).toList())
            );
        }
        if (filter != null && filter.isSetCreatedFrom()) {
            conditions.add(REPORT_JOB.CREATED_AT.ge(toLocalDateTime(TimestampUtils.parse(filter.getCreatedFrom()))));
        }
        if (filter != null && filter.isSetCreatedTo()) {
            conditions.add(REPORT_JOB.CREATED_AT.le(toLocalDateTime(TimestampUtils.parse(filter.getCreatedTo()))));
        }
        if (cursor != null) {
            conditions.add(
                    REPORT_JOB.CREATED_AT.lt(toLocalDateTime(cursor.createdAt()))
                            .or(
                                    REPORT_JOB.CREATED_AT.eq(toLocalDateTime(cursor.createdAt()))
                                            .and(REPORT_JOB.ID.lt(cursor.reportId()))
                            )
            );
        }

        return baseReportSelect()
                .where(conditions)
                .orderBy(REPORT_JOB.CREATED_AT.desc(), REPORT_JOB.ID.desc())
                .limit(limit)
                .fetch(ReportDao::mapReport);
    }

    public boolean cancelReport(String createdBy, long reportId, Instant now) {
        var updated = dslContext.update(REPORT_JOB)
                .set(REPORT_JOB.STATUS, toJooqReportStatus(ReportStatus.canceled))
                .set(
                        REPORT_JOB.FINISHED_AT,
                        org.jooq.impl.DSL.coalesce(REPORT_JOB.FINISHED_AT, toLocalDateTime(now))
                )
                .set(REPORT_JOB.UPDATED_AT, toLocalDateTime(now))
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.CREATED_BY.eq(createdBy))
                .and(
                        REPORT_JOB.STATUS.in(
                                toJooqReportStatus(ReportStatus.pending),
                                toJooqReportStatus(ReportStatus.processing)
                        )
                )
                .execute();
        return updated > 0;
    }

    public boolean reportExists(String createdBy, long reportId) {
        return dslContext.fetchExists(
                dslContext.selectOne()
                        .from(REPORT_JOB)
                        .where(REPORT_JOB.ID.eq(reportId))
                        .and(REPORT_JOB.CREATED_BY.eq(createdBy))
        );
    }

    public Optional<ReportFile> getFile(String createdBy, String fileId) {
        var createdAt = timestampField(REPORT_FILE.CREATED_AT, "created_at_ts");
        return dslContext.select(
                        REPORT_FILE.REPORT_ID,
                        REPORT_FILE.FILE_ID,
                        REPORT_FILE.FILE_TYPE,
                        REPORT_FILE.FILENAME,
                        REPORT_FILE.CONTENT_TYPE,
                        REPORT_FILE.MD5,
                        REPORT_FILE.SHA256,
                        REPORT_FILE.SIZE_BYTES,
                        createdAt,
                        REPORT_FILE.BUCKET,
                        REPORT_FILE.OBJECT_KEY
                )
                .from(REPORT_FILE)
                .join(REPORT_JOB).on(REPORT_JOB.ID.eq(REPORT_FILE.REPORT_ID))
                .where(REPORT_FILE.FILE_ID.eq(fileId))
                .and(REPORT_JOB.CREATED_BY.eq(createdBy))
                .fetchOptional(ReportDao::mapFile);
    }

    private SelectJoinStep<Record> baseReportSelect() {
        var reportFileType = REPORT_FILE.FILE_TYPE.as("report_file_type");
        var createdAt = timestampField(REPORT_JOB.CREATED_AT, "created_at_ts");
        var updatedAt = timestampField(REPORT_JOB.UPDATED_AT, "updated_at_ts");
        var startedAt = timestampField(REPORT_JOB.STARTED_AT, "started_at_ts");
        var dataSnapshotFixedAt = timestampField(REPORT_JOB.DATA_SNAPSHOT_FIXED_AT, "data_snapshot_fixed_at_ts");
        var finishedAt = timestampField(REPORT_JOB.FINISHED_AT, "finished_at_ts");
        var expiresAt = timestampField(REPORT_JOB.EXPIRES_AT, "expires_at_ts");
        var fileCreatedAt = timestampField(REPORT_FILE.CREATED_AT, "file_created_at_ts");
        return dslContext.select(
                        REPORT_JOB.ID,
                        REPORT_JOB.REPORT_TYPE,
                        REPORT_JOB.FILE_TYPE,
                        REPORT_JOB.QUERY_JSON,
                        REPORT_JOB.TIMEZONE,
                        REPORT_JOB.STATUS,
                        createdAt,
                        updatedAt,
                        startedAt,
                        dataSnapshotFixedAt,
                        finishedAt,
                        REPORT_JOB.ROWS_COUNT,
                        expiresAt,
                        REPORT_JOB.ERROR_CODE,
                        REPORT_JOB.ERROR_MESSAGE,
                        REPORT_FILE.FILE_ID,
                        reportFileType,
                        REPORT_FILE.FILENAME,
                        REPORT_FILE.CONTENT_TYPE,
                        REPORT_FILE.MD5,
                        REPORT_FILE.SHA256,
                        REPORT_FILE.SIZE_BYTES,
                        fileCreatedAt
                )
                .from(REPORT_JOB)
                .leftJoin(REPORT_FILE).on(REPORT_FILE.REPORT_ID.eq(REPORT_JOB.ID));
    }

    private static StoredReport mapReport(Record record) {
        var reportFileType = REPORT_FILE.FILE_TYPE.as("report_file_type");
        var createdAt = timestampField(REPORT_JOB.CREATED_AT, "created_at_ts");
        var updatedAt = timestampField(REPORT_JOB.UPDATED_AT, "updated_at_ts");
        var startedAt = timestampField(REPORT_JOB.STARTED_AT, "started_at_ts");
        var dataSnapshotFixedAt = timestampField(REPORT_JOB.DATA_SNAPSHOT_FIXED_AT, "data_snapshot_fixed_at_ts");
        var finishedAt = timestampField(REPORT_JOB.FINISHED_AT, "finished_at_ts");
        var expiresAt = timestampField(REPORT_JOB.EXPIRES_AT, "expires_at_ts");
        var fileCreatedAt = timestampField(REPORT_FILE.CREATED_AT, "file_created_at_ts");
        return new StoredReport(
                record.get(REPORT_JOB.ID),
                fromJooqReportType(record.get(REPORT_JOB.REPORT_TYPE)),
                fromJooqFileType(record.get(REPORT_JOB.FILE_TYPE)),
                record.get(REPORT_JOB.QUERY_JSON).data(),
                record.get(REPORT_JOB.TIMEZONE),
                fromJooqReportStatus(record.get(REPORT_JOB.STATUS)),
                TimestampUtils.toInstant(record.get(createdAt)),
                TimestampUtils.toInstant(record.get(updatedAt)),
                TimestampUtils.toOptionalInstant(record.get(startedAt)),
                TimestampUtils.toOptionalInstant(record.get(dataSnapshotFixedAt)),
                TimestampUtils.toOptionalInstant(record.get(finishedAt)),
                record.get(REPORT_JOB.ROWS_COUNT),
                TimestampUtils.toOptionalInstant(record.get(expiresAt)),
                record.get(REPORT_JOB.ERROR_CODE),
                record.get(REPORT_JOB.ERROR_MESSAGE),
                record.get(REPORT_FILE.FILE_ID) == null ? null : mapStoredReportFile(
                        record.get(REPORT_FILE.FILE_ID),
                        record.get(reportFileType),
                        record.get(REPORT_FILE.FILENAME),
                        record.get(REPORT_FILE.CONTENT_TYPE),
                        record.get(REPORT_FILE.MD5),
                        record.get(REPORT_FILE.SHA256),
                        record.get(REPORT_FILE.SIZE_BYTES),
                        record.get(fileCreatedAt)
                )
        );
    }

    private static ReportFile mapFile(Record record) {
        var reportFile = new ReportFile();
        reportFile.setReportId(record.get(REPORT_FILE.REPORT_ID));
        reportFile.setFileId(record.get(REPORT_FILE.FILE_ID));
        reportFile.setFileType(record.get(REPORT_FILE.FILE_TYPE));
        reportFile.setFilename(record.get(REPORT_FILE.FILENAME));
        reportFile.setContentType(record.get(REPORT_FILE.CONTENT_TYPE));
        reportFile.setMd5(record.get(REPORT_FILE.MD5));
        reportFile.setSha256(record.get(REPORT_FILE.SHA256));
        reportFile.setSizeBytes(record.get(REPORT_FILE.SIZE_BYTES));
        reportFile.setCreatedAt(TimestampUtils.toLocalDateTime(
                TimestampUtils.toInstant(record.get(timestampField(REPORT_FILE.CREATED_AT, "created_at_ts")))
        ));
        reportFile.setBucket(record.get(REPORT_FILE.BUCKET));
        reportFile.setObjectKey(record.get(REPORT_FILE.OBJECT_KEY));
        return reportFile;
    }

    private static StoredReportFile mapStoredReportFile(
            String fileId,
            dev.vality.ccreporter.domain.enums.FileType fileType,
            String filename,
            String contentType,
            String md5,
            String sha256,
            Long sizeBytes,
            Timestamp createdAt
    ) {
        return new StoredReportFile(
                fileId,
                fromJooqFileType(fileType),
                filename,
                contentType,
                md5,
                sha256,
                sizeBytes,
                TimestampUtils.toInstant(createdAt)
        );
    }

    private static LocalDateTime toLocalDateTime(Instant value) {
        return TimestampUtils.toLocalDateTime(value);
    }

    private static org.jooq.Field<Timestamp> timestampField(TableField<?, ?> field, String alias) {
        return org.jooq.impl.DSL.field(field.getQualifiedName(), Timestamp.class).as(alias);
    }

    private static dev.vality.ccreporter.domain.enums.ReportStatus toJooqReportStatus(ReportStatus status) {
        return dev.vality.ccreporter.domain.enums.ReportStatus.valueOf(status.name());
    }

    private static dev.vality.ccreporter.domain.enums.ReportType toJooqReportType(ReportType reportType) {
        return dev.vality.ccreporter.domain.enums.ReportType.valueOf(reportType.name());
    }

    private static dev.vality.ccreporter.domain.enums.FileType toJooqFileType(FileType fileType) {
        return dev.vality.ccreporter.domain.enums.FileType.valueOf(fileType.name());
    }

    private static ReportStatus fromJooqReportStatus(dev.vality.ccreporter.domain.enums.ReportStatus status) {
        return ReportStatus.valueOf(status.getLiteral());
    }

    private static ReportType fromJooqReportType(dev.vality.ccreporter.domain.enums.ReportType reportType) {
        return ReportType.valueOf(reportType.getLiteral());
    }

    private static FileType fromJooqFileType(dev.vality.ccreporter.domain.enums.FileType fileType) {
        return FileType.valueOf(fileType.getLiteral());
    }
}
