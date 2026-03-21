package dev.vality.ccreporter.dao;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.domain.tables.pojos.ReportAuditEvent;
import dev.vality.ccreporter.model.RequestAuditMetadata;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;

import static dev.vality.ccreporter.domain.Tables.REPORT_AUDIT_EVENT;

@Repository
@RequiredArgsConstructor
public class ReportAuditDao {

    private final DSLContext dslContext;
    private final ObjectMapper objectMapper;

    public void insertEvent(
            long reportId,
            String eventType,
            String actor,
            RequestAuditMetadata metadata,
            Object details
    ) {
        var payloadJson = serializePayload(metadata, details);
        var auditEvent = new ReportAuditEvent()
                .setReportId(reportId)
                .setEventType(eventType)
                .setActor(actor)
                .setPayloadJson(JSONB.jsonb(payloadJson));
        var record = dslContext.newRecord(REPORT_AUDIT_EVENT, auditEvent);
        record.changed(REPORT_AUDIT_EVENT.ID, false);
        record.changed(REPORT_AUDIT_EVENT.CREATED_AT, false);
        dslContext.insertInto(REPORT_AUDIT_EVENT)
                .set(record)
                .execute();
    }

    private String serializePayload(RequestAuditMetadata metadata, Object details) {
        try {
            return objectMapper.writeValueAsString(new AuditPayload(
                    metadata,
                    details
            ));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize report audit payload", ex);
        }
    }

    private record AuditPayload(
            @JsonUnwrapped RequestAuditMetadata metadata,
            Object details
    ) {
    }
}
