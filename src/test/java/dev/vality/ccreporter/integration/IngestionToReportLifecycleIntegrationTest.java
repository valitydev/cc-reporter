package dev.vality.ccreporter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.GetReportsRequest;
import dev.vality.ccreporter.Report;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.TimeRange;
import dev.vality.ccreporter.ingestion.PaymentIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalIngestionService;
import dev.vality.ccreporter.ingestion.WithdrawalSessionIngestionService;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

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
        String url = reportingHandler.generatePresignedUrl(new GeneratePresignedUrlRequest(report.getFile().getFileId()));

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
        String url = reportingHandler.generatePresignedUrl(new GeneratePresignedUrlRequest(report.getFile().getFileId()));

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
        var request = createPaymentsReportRequest("ingestion-payments-lifecycle-1");
        request.getQuery().getPayments().setTimeRange(new TimeRange(
                "2025-12-31T00:00:00Z",
                "2026-01-02T00:00:00Z"
        ));
        return request;
    }

    private dev.vality.ccreporter.CreateReportRequest withdrawalsLifecycleRequest() {
        var request = createWithdrawalsReportRequest("ingestion-withdrawals-lifecycle-1");
        request.getQuery().getWithdrawals().setTimeRange(new TimeRange(
                "2025-12-31T00:00:00Z",
                "2026-01-02T00:00:00Z"
        ));
        return request;
    }
}
