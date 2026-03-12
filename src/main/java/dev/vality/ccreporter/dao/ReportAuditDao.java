package dev.vality.ccreporter.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.security.RequestAuditMetadata;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.LinkedHashMap;
import java.util.Map;

@Repository
public class ReportAuditDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public ReportAuditDao(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
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
        jdbcTemplate.update(
                """
                        INSERT INTO ccr.report_audit_event (
                            report_id,
                            event_type,
                            actor,
                            payload_json
                        )
                        VALUES (
                            :reportId,
                            :eventType,
                            :actor,
                            CAST(:payloadJson AS jsonb)
                        )
                        """,
                new MapSqlParameterSource()
                        .addValue("reportId", reportId)
                        .addValue("eventType", eventType)
                        .addValue("actor", actor)
                        .addValue("payloadJson", payloadJson)
        );
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
