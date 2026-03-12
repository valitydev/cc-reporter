package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.ReportRecordFixtures;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет выдачу ссылки на скачивание и то, какие параметры сервис отдаёт в file storage.
 */
class PresignedUrlIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void generatePresignedUrlUsesConfiguredTtlCap() throws Exception {
        long reportId = reportingHandler.createReport(ReportRequestFixtures.payments("url-1"));
        ReportRecordFixtures.attachCsvFile(jdbcTemplate, reportId, "file-1", Instant.parse("2026-01-05T10:00:00Z"));

        Instant beforeCall = Instant.now();
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest("file-1");
        request.setRequestedExpiresAt(beforeCall.plus(2, ChronoUnit.HOURS).toString());
        String url = reportingHandler.generatePresignedUrl(request);

        assertThat(url).isEqualTo("https://download.example/file-1");
        assertThat(stubFileStorageClient.getLastFileId()).isEqualTo("file-1");
        assertThat(stubFileStorageClient.getLastExpiresAt())
                .isAfter(beforeCall.plus(14, ChronoUnit.MINUTES))
                .isBeforeOrEqualTo(beforeCall.plus(15, ChronoUnit.MINUTES).plusSeconds(5));
    }
}
