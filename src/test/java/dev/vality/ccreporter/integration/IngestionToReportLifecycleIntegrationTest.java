package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.ingestion.PaymentIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalSessionIngestionService;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import dev.vality.ccreporter.integration.fixture.SerializedIngestionEventFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Сквозной сценарий от ingestion до готового отчёта, чтобы вся цепочка проверялась одним прогоном.
 */
class IngestionToReportLifecycleIntegrationTest extends AbstractReportingIntegrationTest {

    @Autowired
    private PaymentIngestionService paymentIngestionService;

    @Autowired
    private WithdrawalIngestionService withdrawalIngestionService;

    @Autowired
    private WithdrawalSessionIngestionService withdrawalSessionIngestionService;

    @Test
    void paymentsReportLifecycleRunsFromIngestionToPresignedUrl() throws Exception {
        paymentIngestionService.handleEvents(SerializedIngestionEventFixtures.paymentEvents());

        long reportId = reportingHandler.createReport(paymentsLifecycleRequest());
        boolean processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        String csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );
        String url =
                reportingHandler.generatePresignedUrl(new GeneratePresignedUrlRequest(report.getFile().getFileId()));

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("invoice_id,payment_id");
        assertThat(csv).contains(
                SerializedIngestionEventFixtures.PAYMENT_INVOICE_ID + "," + SerializedIngestionEventFixtures.PAYMENT_ID
        );
        assertThat(csv).contains("trx-payment-1");
        assertThat(url).isEqualTo("https://download.example/" + report.getFile().getFileId());
        assertThat(reportingHandler.getReports(new GetReportsRequest()).getReports())
                .extracting(Report::getReportId)
                .contains(reportId);
    }

    @Test
    void withdrawalsReportLifecycleRunsFromIngestionToPresignedUrl() throws Exception {
        withdrawalIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalEvents());
        withdrawalSessionIngestionService.handleEvents(SerializedIngestionEventFixtures.withdrawalSessionEvents());

        long reportId = reportingHandler.createReport(withdrawalsLifecycleRequest());
        boolean processed = reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"));

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        String csv = new String(
                stubFileStorageClient.getStoredContent(report.getFile().getFileId()),
                StandardCharsets.UTF_8
        );
        String url =
                reportingHandler.generatePresignedUrl(new GeneratePresignedUrlRequest(report.getFile().getFileId()));

        assertThat(processed).isTrue();
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(csv).contains("withdrawal_id,party_id");
        assertThat(csv).contains(SerializedIngestionEventFixtures.WITHDRAWAL_ID + ",party-serialized");
        assertThat(csv).contains("trx-withdrawal-1");
        assertThat(url).isEqualTo("https://download.example/" + report.getFile().getFileId());
        assertThat(reportingHandler.getReports(new GetReportsRequest()).getReports())
                .extracting(Report::getReportId)
                .contains(reportId);
    }

    private dev.vality.ccreporter.CreateReportRequest paymentsLifecycleRequest() {
        return ReportRequestFixtures.payments("ingestion-payments-lifecycle-1", new TimeRange(
                "2025-12-31T00:00:00Z",
                "2026-01-02T00:00:00Z"
        ));
    }

    private dev.vality.ccreporter.CreateReportRequest withdrawalsLifecycleRequest() {
        return ReportRequestFixtures.withdrawals("ingestion-withdrawals-lifecycle-1", new TimeRange(
                "2025-12-31T00:00:00Z",
                "2026-01-02T00:00:00Z"
        ));
    }
}
