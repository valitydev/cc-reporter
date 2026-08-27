package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.dao.mapper.ReportAuditMapper;
import dev.vality.ccreporter.model.RequestAuditMetadata;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import static dev.vality.ccreporter.domain.Tables.REPORT_AUDIT_EVENT;

@Repository
@RequiredArgsConstructor
public class ReportAuditDao {

    private final DSLContext dslContext;
    private final ReportAuditMapper reportAuditMapper;

    public void insertEvent(
            long reportId,
            String eventType,
            String actor,
            RequestAuditMetadata metadata,
            Object details
    ) {
        dslContext.insertInto(REPORT_AUDIT_EVENT)
                .set(reportAuditMapper.newInsertableRecord(
                        dslContext,
                        reportId,
                        eventType,
                        actor,
                        metadata,
                        details
                ))
                .execute();
    }
}
