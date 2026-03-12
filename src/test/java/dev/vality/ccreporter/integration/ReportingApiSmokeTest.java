package dev.vality.ccreporter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.vality.ccreporter.CancelReportRequest;
import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.GetReportsMeta;
import dev.vality.ccreporter.GetReportsRequest;
import dev.vality.ccreporter.GetReportsResponse;
import dev.vality.ccreporter.Report;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.ReportType;
import org.junit.jupiter.api.Test;

class ReportingApiSmokeTest extends AbstractReportingIntegrationTest {

    @Test
    void applicationStartsWithDatabase() {
        Integer result = jdbcTemplate.queryForObject("select 1", Integer.class);
        Integer flywayHistoryTableCount = jdbcTemplate.queryForObject(
                "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
                Integer.class,
                "ccr",
                "flyway_schema_history"
        );
        Integer reportJobTableCount = jdbcTemplate.queryForObject(
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
        CreateReportRequest request = createPaymentsReportRequest("idem-1");

        long firstId = reportingHandler.createReport(request);
        long secondId = reportingHandler.createReport(request);
        Report report = reportingHandler.getReport(new GetReportRequest(firstId));

        assertThat(secondId).isEqualTo(firstId);
        assertThat(report.getReportId()).isEqualTo(firstId);
        assertThat(report.getStatus()).isEqualTo(ReportStatus.pending);
        assertThat(report.getReportType()).isEqualTo(ReportType.payments);
        assertThat(report.getQuery().isSetPayments()).isTrue();
    }

    @Test
    void getReportsReturnsContinuationToken() throws Exception {
        reportingHandler.createReport(createPaymentsReportRequest("page-1"));
        reportingHandler.createReport(createPaymentsReportRequest("page-2"));

        GetReportsMeta meta = new GetReportsMeta();
        meta.setLimit(1);
        GetReportsResponse response = reportingHandler.getReports(new GetReportsRequest().setMeta(meta));

        assertThat(response.getReports()).hasSize(1);
        assertThat(response.getContinuationToken()).isNotBlank();
    }

    @Test
    void cancelReportIsIdempotentForPendingReport() throws Exception {
        long reportId = reportingHandler.createReport(createPaymentsReportRequest("cancel-1"));

        reportingHandler.cancelReport(new CancelReportRequest(reportId));
        reportingHandler.cancelReport(new CancelReportRequest(reportId));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.canceled);
        assertThat(report.getFinishedAt()).isNotBlank();
    }
}
