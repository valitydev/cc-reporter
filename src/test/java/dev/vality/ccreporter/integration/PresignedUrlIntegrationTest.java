package dev.vality.ccreporter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

class PresignedUrlIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void generatePresignedUrlUsesConfiguredTtlCap() throws Exception {
        long reportId = reportingHandler.createReport(createPaymentsReportRequest("url-1"));
        attachCsvFile(reportId, "file-1", Instant.parse("2026-01-05T10:00:00Z"));

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
