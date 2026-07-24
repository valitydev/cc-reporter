package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.CancelReportRequest;
import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import dev.vality.ccreporter.fixture.ReportRecordFixtures;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
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

        assertThat(auditRow.get("actor")).isEqualTo("alice@example.com");
        assertThat(auditRow.get("event_type")).isEqualTo("report_created");
        assertThat(jsonText(reportId, "report_created", "{userId}")).isEqualTo("user-id-42");
        assertThat(jsonText(reportId, "report_created", "{username}")).isEqualTo("alice");
        assertThat(jsonText(reportId, "report_created", "{email}")).isEqualTo("alice@example.com");
        assertThat(jsonText(reportId, "report_created", "{traceId}")).isEqualTo("audit-trace-id");
        assertThat(jsonText(reportId, "report_created", "{traceparent}"))
                .isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00aa0ba902b7-01");
        assertThat(
                jsonText(reportId, "report_created", "{details,idempotencyKey}")
        ).isEqualTo("audit-create-1");
    }

    @Test
    void cancelReportAndPresignedUrlWriteAuditEvents() throws Exception {
        bindCallerWithAuditMetadata("user-9");
        var canceledReportId = reportingHandler.createReport(ReportRequestFixtures.payments("audit-cancel-1"));
        var downloadableReportId = reportingHandler.createReport(ReportRequestFixtures.payments("audit-url-1"));
        var now = Instant.now();
        ReportRecordFixtures.markReportCreated(
                jdbcTemplate,
                downloadableReportId,
                now.minusSeconds(120),
                now.minusSeconds(120),
                now.minusSeconds(60),
                now.plus(1, ChronoUnit.HOURS),
                1L
        );
        ReportRecordFixtures.attachCsvFile(
                jdbcTemplate,
                downloadableReportId,
                "file-audit-1",
                now.minusSeconds(60)
        );

        reportingHandler.cancelReport(new CancelReportRequest(canceledReportId));

        var request = new GeneratePresignedUrlRequest(
                "file-audit-1"
        );
        request.setRequestedExpiresAt(Instant.now().plus(2, ChronoUnit.HOURS).toString());
        reportingHandler.generatePresignedUrl(request);

        var cancelAudit = findLatestAudit(canceledReportId, "report_canceled");
        assertThat(cancelAudit.get("actor")).isEqualTo("alice@example.com");
        assertThat(jsonText(canceledReportId, "report_canceled", "{details,stateChanged}")).isEqualTo("true");

        var presignedAudit = findLatestAudit(downloadableReportId, "presigned_url_generated");
        assertThat(presignedAudit.get("actor")).isEqualTo("alice@example.com");
        assertThat(jsonText(downloadableReportId, "presigned_url_generated", "{details,fileId}"))
                .isEqualTo("file-audit-1");
        assertThat(jsonText(downloadableReportId, "presigned_url_generated", "{details,requestedExpiresAt}"))
                .isNotBlank();
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
