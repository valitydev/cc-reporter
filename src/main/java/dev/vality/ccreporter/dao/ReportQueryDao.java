package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.GetReportsFilter;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.model.StoredReport;
import dev.vality.ccreporter.serde.json.ContinuationTokenJsonSerializer.PageCursor;
import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectJoinStep;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;
import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;
import static dev.vality.ccreporter.util.TimestampUtils.parse;
import static dev.vality.ccreporter.util.TimestampUtils.toLocalDateTime;

@Repository
@RequiredArgsConstructor
public class ReportQueryDao {

    private final DSLContext dslContext;

    public Optional<StoredReport> getReport(String createdBy, long reportId) {
        return baseReportSelect()
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.CREATED_BY.eq(createdBy))
                .fetchOptional(ReportRecordMapper::mapReport);
    }

    public List<StoredReport> getReports(String createdBy, GetReportsFilter filter, PageCursor cursor, int limit) {
        var conditions = new ArrayList<Condition>();
        conditions.add(REPORT_JOB.CREATED_BY.eq(createdBy));

        if (filter != null && filter.isSetStatuses() && !filter.getStatuses().isEmpty()) {
            conditions.add(
                    REPORT_JOB.STATUS.in(
                            filter.getStatuses().stream().map(ReportRecordMapper::toJooqReportStatus).toList()
                    )
            );
        }
        if (filter != null && filter.isSetReportTypes() && !filter.getReportTypes().isEmpty()) {
            conditions.add(
                    REPORT_JOB.REPORT_TYPE.in(
                            filter.getReportTypes().stream().map(ReportRecordMapper::toJooqReportType).toList()
                    )
            );
        }
        if (filter != null && filter.isSetFileTypes() && !filter.getFileTypes().isEmpty()) {
            conditions.add(
                    REPORT_JOB.FILE_TYPE.in(
                            filter.getFileTypes().stream().map(ReportRecordMapper::toJooqFileType).toList()
                    )
            );
        }
        if (filter != null && filter.isSetCreatedFrom()) {
            conditions.add(REPORT_JOB.CREATED_AT.ge(toLocalDateTime(parse(filter.getCreatedFrom()))));
        }
        if (filter != null && filter.isSetCreatedTo()) {
            conditions.add(REPORT_JOB.CREATED_AT.le(toLocalDateTime(parse(filter.getCreatedTo()))));
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
                .fetch(ReportRecordMapper::mapReport);
    }

    public Optional<ReportFile> getFile(String createdBy, String fileId) {
        var createdAt = ReportRecordMapper.timestampField(REPORT_FILE.CREATED_AT, "created_at_ts");
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
                .fetchOptional(ReportRecordMapper::mapFile);
    }

    private SelectJoinStep<Record> baseReportSelect() {
        var reportFileType = REPORT_FILE.FILE_TYPE.as("report_file_type");
        var createdAt = ReportRecordMapper.timestampField(REPORT_JOB.CREATED_AT, "created_at_ts");
        var updatedAt = ReportRecordMapper.timestampField(REPORT_JOB.UPDATED_AT, "updated_at_ts");
        var startedAt = ReportRecordMapper.timestampField(REPORT_JOB.STARTED_AT, "started_at_ts");
        var dataSnapshotFixedAt =
                ReportRecordMapper.timestampField(REPORT_JOB.DATA_SNAPSHOT_FIXED_AT, "data_snapshot_fixed_at_ts");
        var finishedAt = ReportRecordMapper.timestampField(REPORT_JOB.FINISHED_AT, "finished_at_ts");
        var expiresAt = ReportRecordMapper.timestampField(REPORT_JOB.EXPIRES_AT, "expires_at_ts");
        var fileCreatedAt = ReportRecordMapper.timestampField(REPORT_FILE.CREATED_AT, "file_created_at_ts");
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
}
