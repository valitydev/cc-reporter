package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Короткая проверка базового контракта API: сервис стартует, создаёт отчёт и умеет его читать обратно.
 */
class ReportingApiSmokeTest extends AbstractReportingIntegrationTest {

    @Test
    void applicationStartsWithDatabase() {
        var result = jdbcTemplate.queryForObject("select 1", Integer.class);
        var flywayHistoryTableCount = jdbcTemplate.queryForObject(
                "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
                Integer.class,
                "ccr",
                "flyway_schema_history"
        );
        var reportJobTableCount = jdbcTemplate.queryForObject(
                "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
                Integer.class,
                "ccr",
                "report_job"
        );

        assertThat(result).isEqualTo(1);
        assertThat(flywayHistoryTableCount).isEqualTo(1);
        assertThat(reportJobTableCount).isEqualTo(1);
    }

    @Test
    void createReportIsIdempotentAndReadable() throws Exception {
        var request = ReportRequestFixtures.payments("idem-1");

        var firstId = reportingHandler.createReport(request);
        var secondId = reportingHandler.createReport(request);
        var report = reportingHandler.getReport(new GetReportRequest(firstId));

        assertThat(secondId).isEqualTo(firstId);
        assertThat(report.getReportId()).isEqualTo(firstId);
        assertThat(report.getStatus()).isEqualTo(ReportStatus.pending);
        assertThat(report.getReportType()).isEqualTo(ReportType.payments);
        assertThat(report.getQuery().isSetPayments()).isTrue();
    }

    @Test
    void getReportsReturnsContinuationToken() throws Exception {
        reportingHandler.createReport(ReportRequestFixtures.payments("page-1"));
        reportingHandler.createReport(ReportRequestFixtures.payments("page-2"));

        var meta = new GetReportsMeta();
        meta.setLimit(1);
        var response = reportingHandler.getReports(new GetReportsRequest().setMeta(meta));

        assertThat(response.getReports()).hasSize(1);
        assertThat(response.getContinuationToken()).isNotBlank();
    }

    @Test
    void cancelReportIsIdempotentForPendingReport() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("cancel-1"));

        reportingHandler.cancelReport(new CancelReportRequest(reportId));
        reportingHandler.cancelReport(new CancelReportRequest(reportId));

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.canceled);
        assertThat(report.getFinishedAt()).isNotBlank();
    }
}
