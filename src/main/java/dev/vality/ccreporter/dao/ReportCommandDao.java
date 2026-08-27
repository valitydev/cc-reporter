package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.dao.mapper.ReportRecordMapper;
import dev.vality.ccreporter.serde.json.ThriftJsonCodec;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import java.util.Optional;

import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;

@Repository
@RequiredArgsConstructor
public class ReportCommandDao {

    private final DSLContext dslContext;
    private final ThriftJsonCodec thriftJsonCodec;

    public CreateResult createReport(
            String createdBy,
            ReportType reportType,
            FileType fileType,
            ReportQuery query,
            String timezone,
            String idempotencyKey
    ) {
        var normalizedIdempotencyKey = StringUtils.hasText(idempotencyKey) ? idempotencyKey : null;
        var insertedId = dslContext.insertInto(REPORT_JOB)
                .columns(
                        REPORT_JOB.REPORT_TYPE,
                        REPORT_JOB.FILE_TYPE,
                        REPORT_JOB.QUERY_JSON,
                        REPORT_JOB.TIMEZONE,
                        REPORT_JOB.CREATED_BY,
                        REPORT_JOB.IDEMPOTENCY_KEY
                )
                .values(
                        ReportRecordMapper.mapEnum(
                                reportType,
                                dev.vality.ccreporter.domain.enums.ReportType.class
                        ),
                        ReportRecordMapper.mapEnum(
                                fileType,
                                dev.vality.ccreporter.domain.enums.FileType.class
                        ),
                        JSONB.jsonb(thriftJsonCodec.serialize(query)),
                        timezone,
                        createdBy,
                        normalizedIdempotencyKey
                )
                .onConflictDoNothing()
                .returningResult(REPORT_JOB.ID)
                .fetchOptional(REPORT_JOB.ID);

        if (insertedId.isPresent()) {
            return new CreateResult(insertedId.get(), true);
        }
        if (normalizedIdempotencyKey == null) {
            throw new IllegalStateException("Report insert returned no id");
        }
        return findByIdempotencyKey(createdBy, normalizedIdempotencyKey)
                .map(reportId -> new CreateResult(reportId, false))
                .orElseThrow(() -> new IllegalStateException("Conflicting report was not found"));
    }

    public boolean reportExists(String createdBy, long reportId) {
        return dslContext.fetchExists(
                dslContext.selectOne()
                        .from(REPORT_JOB)
                        .where(REPORT_JOB.ID.eq(reportId))
                        .and(REPORT_JOB.CREATED_BY.eq(createdBy))
        );
    }

    private Optional<Long> findByIdempotencyKey(String createdBy, String idempotencyKey) {
        return dslContext.select(REPORT_JOB.ID)
                .from(REPORT_JOB)
                .where(REPORT_JOB.CREATED_BY.eq(createdBy))
                .and(REPORT_JOB.IDEMPOTENCY_KEY.eq(idempotencyKey))
                .fetchOptional(REPORT_JOB.ID);
    }

    public record CreateResult(long reportId, boolean created) {
    }
}
