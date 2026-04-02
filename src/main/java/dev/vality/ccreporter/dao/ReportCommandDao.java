package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.dao.mapper.ReportCommandMapper;
import dev.vality.ccreporter.report.ReportQueryService;
import dev.vality.ccreporter.serde.json.ThriftJsonCodec;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.exception.IntegrityConstraintViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.Optional;

import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;

@Repository
@RequiredArgsConstructor
public class ReportCommandDao {

    private final DSLContext dslContext;
    private final ReportQueryService reportQueryService;
    private final ThriftJsonCodec thriftJsonCodec;

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
        var querySpec = reportQueryService.resolveQuerySpec(query);
        var queryJson = thriftJsonCodec.serialize(query);
        var queryHash = reportQueryService.hash(queryJson);
        var reportJob = ReportCommandMapper.mapReportJob(
                createdBy,
                reportType,
                fileType,
                queryJson,
                queryHash,
                querySpec.timeRange().from(),
                querySpec.timeRange().to(),
                timezone,
                idempotencyKey
        );

        try {
            return Objects.requireNonNull(
                    dslContext.insertInto(REPORT_JOB)
                            .set(ReportCommandMapper.newInsertableRecord(dslContext, reportJob))
                            .returningResult(REPORT_JOB.ID)
                            .fetchOne(REPORT_JOB.ID),
                    "Report creation must return an id"
            );
        } catch (IntegrityConstraintViolationException ex) {
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
