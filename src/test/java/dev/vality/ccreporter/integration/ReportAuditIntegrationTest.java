package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.CancelReportRequest;
import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.ReportRecordFixtures;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReportAuditIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void createReportWritesAuditEventWithTrustedRequestMetadata() throws Exception {
        bindCallerWithAuditMetadata("user-7");

        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("audit-create-1"));

        var auditRow = findLatestAudit(reportId, "report_created");

        assertThat(auditRow.get("actor")).isEqualTo("user-7");
        assertThat(auditRow.get("event_type")).isEqualTo("report_created");
        assertThat(jsonText(reportId, "report_created", "{actor,id}")).isEqualTo("user-id-42");
        assertThat(jsonText(reportId, "report_created", "{actor,username}")).isEqualTo("alice");
        assertThat(jsonText(reportId, "report_created", "{actor,email}")).isEqualTo("alice@example.com");
        assertThat(jsonText(reportId, "report_created", "{request,id}")).isEqualTo("req-123");
        assertThat(jsonText(reportId, "report_created", "{trace,traceparent}"))
                .isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00aa0ba902b7-01");
        assertThat(
                jsonText(reportId, "report_created", "{details,idempotencyKey}")
        ).isEqualTo("audit-create-1");
        assertThat(jsonText(reportId, "report_created", "{details,idempotentReplay}")).isEqualTo("false");
    }

    @Test
    void cancelReportAndPresignedUrlWriteAuditEvents() throws Exception {
        bindCallerWithAuditMetadata("user-9");
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("audit-actions-1"));
        var fileCreatedAt = Instant.parse("2026-01-05T10:00:00Z");
        ReportRecordFixtures.attachCsvFile(jdbcTemplate, reportId, "file-audit-1", fileCreatedAt);

        reportingHandler.cancelReport(new CancelReportRequest(reportId));

        var request = new GeneratePresignedUrlRequest(
                "file-audit-1"
        );
        request.setRequestedExpiresAt(Instant.now().plus(2, ChronoUnit.HOURS).toString());
        reportingHandler.generatePresignedUrl(request);

        var cancelAudit = findLatestAudit(reportId, "report_canceled");
        assertThat(cancelAudit.get("actor")).isEqualTo("user-9");
        assertThat(jsonText(reportId, "report_canceled", "{details,state_changed}")).isEqualTo("true");
        assertThat(jsonText(reportId, "report_canceled", "{request,deadline}")).isEqualTo("2026-01-05T10:15:30Z");

        var presignedAudit = findLatestAudit(reportId, "presigned_url_generated");
        assertThat(presignedAudit.get("actor")).isEqualTo("user-9");
        assertThat(jsonText(reportId, "presigned_url_generated", "{details,fileId}")).isEqualTo("file-audit-1");
        assertThat(jsonText(reportId, "presigned_url_generated", "{details,requestedExpiresAt}")).isNotBlank();
        assertThat(presignedAudit.get("created_at")).isNotNull();
    }

    private Map<String, Object> findLatestAudit(long reportId, String eventType) {
        return jdbcTemplate.queryForMap(
                """
                        SELECT actor, event_type, created_at
                        FROM ccr.report_audit_event
                        WHERE report_id = ? AND event_type = ?
                        ORDER BY created_at DESC, id DESC
                        LIMIT 1
                        """,
                reportId,
                eventType
        );
    }

    private String jsonText(long reportId, String eventType, String path) {
        return jdbcTemplate.queryForObject(
                "SELECT payload_json #>> '" + path + "' FROM ccr.report_audit_event " +
                        "WHERE report_id = ? AND event_type = ? ORDER BY created_at DESC, id DESC LIMIT 1",
                String.class,
                reportId,
                eventType
        );
    }
}
