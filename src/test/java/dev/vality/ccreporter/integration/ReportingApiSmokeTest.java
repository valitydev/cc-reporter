package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void reportStorageDoesNotKeepDerivedColumns() {
        var reportJobColumns = jdbcTemplate.queryForList(
                """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'ccr' AND table_name = 'report_job'
                        """,
                String.class
        );
        var reportFileColumns = jdbcTemplate.queryForList(
                """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'ccr' AND table_name = 'report_file'
                        """,
                String.class
        );

        assertThat(reportJobColumns).doesNotContain(
                "query_hash",
                "requested_time_from",
                "requested_time_to",
                "updated_at"
        );
        assertThat(reportFileColumns).doesNotContain("bucket", "object_key");
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
    void reportOwnershipUsesStableUserIdInsteadOfEmail() throws Exception {
        bindCallerIdentity("stable-user-id", "old@example.com");
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("owner-user-id-1"));

        bindCallerIdentity("stable-user-id", "new@example.com");
        var report = reportingHandler.getReport(new GetReportRequest(reportId));

        assertThat(report.getReportId()).isEqualTo(reportId);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT created_by FROM ccr.report_job WHERE id = ?",
                String.class,
                reportId
        )).isEqualTo("stable-user-id");

        bindCallerIdentity("different-user-id", "old@example.com");
        assertThatThrownBy(() -> reportingHandler.getReport(new GetReportRequest(reportId)))
                .isInstanceOf(ReportNotFound.class);
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
    void getReportsOmitsContinuationTokenWhenPageHasNoMoreRows() throws Exception {
        reportingHandler.createReport(ReportRequestFixtures.payments("last-page-1"));

        var meta = new GetReportsMeta();
        meta.setLimit(1);
        var response = reportingHandler.getReports(new GetReportsRequest().setMeta(meta));

        assertThat(response.getReports()).hasSize(1);
        assertThat(response.isSetContinuationToken()).isFalse();
    }

    @Test
    void getReportsValidatesEachCreationTimestampIndependently() {
        var filter = new GetReportsFilter();
        filter.setCreatedFrom("not-an-instant");

        assertThatThrownBy(() -> reportingHandler.getReports(new GetReportsRequest().setFilter(filter)))
                .isInstanceOf(InvalidRequest.class);
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
