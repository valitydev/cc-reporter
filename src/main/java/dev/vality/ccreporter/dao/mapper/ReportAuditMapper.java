package dev.vality.ccreporter.dao.mapper;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.domain.tables.pojos.ReportAuditEvent;
import dev.vality.ccreporter.domain.tables.records.ReportAuditEventRecord;
import dev.vality.ccreporter.model.RequestAuditMetadata;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Component;

import static dev.vality.ccreporter.domain.Tables.REPORT_AUDIT_EVENT;

@Component
@RequiredArgsConstructor
public class ReportAuditMapper {

    private final ObjectMapper objectMapper;

    public ReportAuditEventRecord newInsertableRecord(
            DSLContext dslContext,
            long reportId,
            String eventType,
            String actor,
            RequestAuditMetadata metadata,
            Object details
    ) {
        var auditEvent = new ReportAuditEvent()
                .setReportId(reportId)
                .setEventType(eventType)
                .setActor(actor)
                .setPayloadJson(JSONB.jsonb(serializePayload(metadata, details)));
        var record = dslContext.newRecord(REPORT_AUDIT_EVENT, auditEvent);
        record.changed(REPORT_AUDIT_EVENT.ID, false);
        record.changed(REPORT_AUDIT_EVENT.CREATED_AT, false);
        return record;
    }

    private String serializePayload(RequestAuditMetadata metadata, Object details) {
        try {
            return objectMapper.writeValueAsString(new AuditPayload(metadata, details));
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
