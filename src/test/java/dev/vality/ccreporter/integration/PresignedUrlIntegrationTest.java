package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import dev.vality.ccreporter.fixture.ReportRecordFixtures;
import dev.vality.ccreporter.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Проверяет выдачу ссылки на скачивание и то, какие параметры сервис отдаёт в file storage.
 */
class PresignedUrlIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void generatePresignedUrlUsesConfiguredTtlCap() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("url-1"));
        var beforeCall = Instant.now();
        ReportRecordFixtures.markReportCreated(
                jdbcTemplate,
                reportId,
                beforeCall.minusSeconds(120),
                beforeCall.minusSeconds(120),
                beforeCall.minusSeconds(60),
                beforeCall.plus(1, ChronoUnit.HOURS),
                1L
        );
        ReportRecordFixtures.attachCsvFile(jdbcTemplate, reportId, "file-1", beforeCall.minusSeconds(60));

        var request = new GeneratePresignedUrlRequest("file-1");
        request.setRequestedExpiresAt(beforeCall.plus(2, ChronoUnit.HOURS).toString());
        var url = reportingHandler.generatePresignedUrl(request);

        assertThat(url).isEqualTo("https://download.example/file-1");
        assertThat(stubFileStorageClient.getLastFileId()).isEqualTo("file-1");
        assertThat(stubFileStorageClient.getLastExpiresAt())
                .isAfter(beforeCall.plus(14, ChronoUnit.MINUTES))
                .isBeforeOrEqualTo(beforeCall.plus(15, ChronoUnit.MINUTES).plusSeconds(5));
    }

    @Test
    void generatePresignedUrlDoesNotOutliveReport() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("url-report-expiry-1"));
        var now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        var reportExpiresAt = now.plus(5, ChronoUnit.MINUTES);
        ReportRecordFixtures.markReportCreated(
                jdbcTemplate,
                reportId,
                now.minusSeconds(120),
                now.minusSeconds(120),
                now.minusSeconds(60),
                reportExpiresAt,
                1L
        );
        ReportRecordFixtures.attachCsvFile(jdbcTemplate, reportId, "file-report-expiry-1", now.minusSeconds(60));

        var request = new GeneratePresignedUrlRequest("file-report-expiry-1");
        request.setRequestedExpiresAt(now.plus(1, ChronoUnit.HOURS).toString());
        reportingHandler.generatePresignedUrl(request);

        assertThat(stubFileStorageClient.getLastExpiresAt())
                .isAfter(reportExpiresAt.minusSeconds(1))
                .isBeforeOrEqualTo(reportExpiresAt);
    }

    @Test
    void generatePresignedUrlRejectsFileBeforeReportIsCreated() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("url-pending-1"));
        ReportRecordFixtures.attachCsvFile(jdbcTemplate, reportId, "file-pending-1", Instant.now());

        assertThatThrownBy(() -> reportingHandler.generatePresignedUrl(
                new GeneratePresignedUrlRequest("file-pending-1")
        )).isInstanceOf(dev.vality.ccreporter.FileNotFound.class);
    }

    @Test
    void generatePresignedUrlRejectsExpiredReportFile() throws Exception {
        var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("url-expired-1"));
        var now = Instant.now();
        ReportRecordFixtures.markReportCreated(
                jdbcTemplate,
                reportId,
                now.minus(3, ChronoUnit.HOURS),
                now.minus(3, ChronoUnit.HOURS),
                now.minus(2, ChronoUnit.HOURS),
                now.minus(1, ChronoUnit.HOURS),
                1L
        );
        ReportRecordFixtures.attachCsvFile(jdbcTemplate, reportId, "file-expired-1", now.minus(2, ChronoUnit.HOURS));

        assertThatThrownBy(() -> reportingHandler.generatePresignedUrl(
                new GeneratePresignedUrlRequest("file-expired-1")
        )).isInstanceOf(dev.vality.ccreporter.FileNotFound.class);
    }
}
