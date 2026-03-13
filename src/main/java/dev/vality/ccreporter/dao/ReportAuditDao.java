package dev.vality.ccreporter.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.domain.tables.pojos.ReportAuditEvent;
import dev.vality.ccreporter.security.RequestAuditMetadata;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;

import java.util.LinkedHashMap;
import java.util.Map;

import static dev.vality.ccreporter.domain.Tables.REPORT_AUDIT_EVENT;

@Repository
public class ReportAuditDao {

    private final DSLContext dslContext;
    private final ObjectMapper objectMapper;

    public ReportAuditDao(DSLContext dslContext, ObjectMapper objectMapper) {
        this.dslContext = dslContext;
        this.objectMapper = objectMapper;
    }

    public void insertEvent(
            long reportId,
            String eventType,
            String actor,
            RequestAuditMetadata metadata,
            Map<String, Object> details
    ) {
        var payloadJson = serializePayload(metadata, details);
        var auditEvent = new ReportAuditEvent();
        auditEvent.setReportId(reportId);
        auditEvent.setEventType(eventType);
        auditEvent.setActor(actor);
        auditEvent.setPayloadJson(JSONB.jsonb(payloadJson));
        var record = dslContext.newRecord(REPORT_AUDIT_EVENT, auditEvent);
        record.changed(REPORT_AUDIT_EVENT.ID, false);
        record.changed(REPORT_AUDIT_EVENT.CREATED_AT, false);
        dslContext.insertInto(REPORT_AUDIT_EVENT)
                .set(record)
                .execute();
    }

    private String serializePayload(RequestAuditMetadata metadata, Map<String, Object> details) {
        var payload = new LinkedHashMap<String, Object>();
        payload.put("actor", actorPayload(metadata));
        payload.put("request", requestPayload(metadata));
        payload.put("trace", tracePayload(metadata));
        payload.put("details", details == null ? Map.of() : details);
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize report audit payload", ex);
        }
    }

    private Map<String, Object> actorPayload(RequestAuditMetadata metadata) {
        var actor = new LinkedHashMap<String, Object>();
        actor.put("id", metadata.userId());
        actor.put("username", metadata.username());
        actor.put("email", metadata.email());
        actor.put("realm", metadata.realm());
        return actor;
    }

    private Map<String, Object> requestPayload(RequestAuditMetadata metadata) {
        var request = new LinkedHashMap<String, Object>();
        request.put("id", metadata.requestId());
        request.put("deadline", metadata.requestDeadline());
        return request;
    }

    private Map<String, Object> tracePayload(RequestAuditMetadata metadata) {
        var trace = new LinkedHashMap<String, Object>();
        trace.put("traceparent", metadata.traceparent());
        trace.put("tracestate", metadata.tracestate());
        return trace;
    }
}
