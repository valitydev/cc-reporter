package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.GetReportsFilter;
import dev.vality.ccreporter.dao.mapper.ReportRecordMapper;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.model.ReportProjection;
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

    public Optional<ReportProjection> getReport(String createdBy, long reportId) {
        return baseReportSelect()
                .where(REPORT_JOB.ID.eq(reportId))
                .and(REPORT_JOB.CREATED_BY.eq(createdBy))
                .fetchOptional(ReportRecordMapper::mapReportProjection);
    }

    public List<ReportProjection> getReports(String createdBy, GetReportsFilter filter, PageCursor cursor, int limit) {
        return baseReportSelect()
                .where(buildReportConditions(createdBy, filter, cursor))
                .orderBy(REPORT_JOB.CREATED_AT.desc(), REPORT_JOB.ID.desc())
                .limit(limit)
                .fetch(ReportRecordMapper::mapReportProjection);
    }

    public Optional<ReportFile> getFile(String createdBy, String fileId) {
        return baseFileSelect()
                .where(REPORT_FILE.FILE_ID.eq(fileId))
                .and(REPORT_JOB.CREATED_BY.eq(createdBy))
                .fetchOptional(ReportRecordMapper::mapReportFile);
    }

    private List<Condition> buildReportConditions(String createdBy, GetReportsFilter filter, PageCursor cursor) {
        var conditions = new ArrayList<Condition>();
        conditions.add(REPORT_JOB.CREATED_BY.eq(createdBy));

        if (filter != null && filter.isSetStatuses() && !filter.getStatuses().isEmpty()) {
            conditions.add(
                    REPORT_JOB.STATUS.in(
                            filter.getStatuses().stream()
                                    .map(status -> ReportRecordMapper.mapEnum(
                                            status,
                                            dev.vality.ccreporter.domain.enums.ReportStatus.class
                                    ))
                                    .toList()
                    )
            );
        }
        if (filter != null && filter.isSetReportTypes() && !filter.getReportTypes().isEmpty()) {
            conditions.add(
                    REPORT_JOB.REPORT_TYPE.in(
                            filter.getReportTypes().stream()
                                    .map(reportType -> ReportRecordMapper.mapEnum(
                                            reportType,
                                            dev.vality.ccreporter.domain.enums.ReportType.class
                                    ))
                                    .toList()
                    )
            );
        }
        if (filter != null && filter.isSetFileTypes() && !filter.getFileTypes().isEmpty()) {
            conditions.add(
                    REPORT_JOB.FILE_TYPE.in(
                            filter.getFileTypes().stream()
                                    .map(fileType -> ReportRecordMapper.mapEnum(
                                            fileType,
                                            dev.vality.ccreporter.domain.enums.FileType.class
                                    ))
                                    .toList()
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
        return conditions;
    }

    private SelectJoinStep<Record> baseReportSelect() {
        return dslContext.select(REPORT_JOB.fields())
                .select(REPORT_FILE.fields())
                .from(REPORT_JOB)
                .leftJoin(REPORT_FILE).on(REPORT_FILE.REPORT_ID.eq(REPORT_JOB.ID));
    }

    private SelectJoinStep<Record> baseFileSelect() {
        return dslContext.select(REPORT_FILE.fields())
                .from(REPORT_FILE)
                .join(REPORT_JOB).on(REPORT_JOB.ID.eq(REPORT_FILE.REPORT_ID));
    }
}
