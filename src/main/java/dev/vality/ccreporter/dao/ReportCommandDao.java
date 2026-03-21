package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.domain.tables.pojos.ReportJob;
import dev.vality.ccreporter.report.ReportQueryService;
import dev.vality.ccreporter.serde.json.ReportQueryJsonSerializer;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.Optional;

import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;
import static dev.vality.ccreporter.util.TimestampUtils.toLocalDateTime;

@Repository
@RequiredArgsConstructor
public class ReportCommandDao {

    private final DSLContext dslContext;
    private final ReportQueryService reportQueryService;
    private final ReportQueryJsonSerializer reportQueryJsonSerializer;

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
        var timeRange = reportQueryService.extractTimeRange(query);
        var queryJson = reportQueryJsonSerializer.serialize(query);
        var queryHash = reportQueryService.hash(queryJson);
        var reportJob = new ReportJob()
                .setReportType(ReportRecordMapper.toJooqReportType(reportType))
                .setFileType(ReportRecordMapper.toJooqFileType(fileType))
                .setQueryJson(JSONB.jsonb(queryJson))
                .setQueryHash(queryHash)
                .setRequestedTimeFrom(toLocalDateTime(timeRange.from()))
                .setRequestedTimeTo(toLocalDateTime(timeRange.to()))
                .setTimezone(timezone)
                .setCreatedBy(createdBy)
                .setIdempotencyKey(StringUtils.hasText(idempotencyKey) ? idempotencyKey : null);

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

    public boolean reportExists(String createdBy, long reportId) {
        return dslContext.fetchExists(
                dslContext.selectOne()
                        .from(REPORT_JOB)
                        .where(REPORT_JOB.ID.eq(reportId))
                        .and(REPORT_JOB.CREATED_BY.eq(createdBy))
        );
    }
}
