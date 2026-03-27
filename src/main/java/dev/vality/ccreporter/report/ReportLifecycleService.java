package dev.vality.ccreporter.report;

import dev.vality.ccreporter.config.properties.ReportProperties;
import dev.vality.ccreporter.config.properties.ReportSchedulerProperties;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.model.ClaimedReportJob;
import dev.vality.ccreporter.model.GeneratedCsvReport;
import dev.vality.ccreporter.model.ReportFileMetadata;
import dev.vality.ccreporter.storage.FileStorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class ReportLifecycleService {

    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(30);

    private final ReportLifecycleDao reportLifecycleDao;
    private final ReportCsvService reportCsvService;
    private final FileStorageService fileStorageService;
    private final ReportLifecycleTransactionService reportLifecycleTransactionService;
    private final ReportProperties reportProperties;
    private final ReportSchedulerProperties reportSchedulerProperties;

    public void timeoutStaleProcessingReports() {
        timeoutStaleProcessingReports(Instant.now());
    }

    public int timeoutStaleProcessingReports(Instant now) {
        var staleBefore = now.minusMillis(reportSchedulerProperties.getStaleProcessingTimeoutMs());
        return reportLifecycleDao.timeoutStaleProcessingReports(staleBefore, now);
    }

    public void expireReadyReports() {
        expireReadyReports(Instant.now());
    }

    public int expireReadyReports(Instant now) {
        return reportLifecycleDao.expireReports(now);
    }

    public void processNextPendingReport() {
        processNextPendingReport(Instant.now());
    }

    public boolean processNextPendingReport(Instant now) {
        var claimedReportJob = reportLifecycleDao.claimNextPendingReport(now);
        if (claimedReportJob.isEmpty()) {
            return false;
        }
        processClaimedReport(claimedReportJob.get(), now);
        return true;
    }

    private void processClaimedReport(ClaimedReportJob claimedReportJob, Instant processingTime) {
        var generatedCsvReport = (GeneratedCsvReport) null;
        try {
            generatedCsvReport = reportCsvService.generate(claimedReportJob);
            var expiresAt = processingTime.plusSeconds(reportProperties.getExpirationSec());
            var fileId = fileStorageService.storeFile(
                    generatedCsvReport.fileName(),
                    generatedCsvReport.contentType(),
                    generatedCsvReport.contentPath(),
                    expiresAt
            );
            var finishedAt = Instant.now();
            var fileMetadata = buildFileMetadata(fileId, generatedCsvReport);
            reportLifecycleTransactionService.publishCompletedReport(
                    claimedReportJob.id(),
                    fileMetadata,
                    finishedAt,
                    expiresAt,
                    generatedCsvReport
            );
        } catch (Exception ex) {
            handleProcessingFailure(claimedReportJob, processingTime, ex);
        } finally {
            deleteStagedFile(generatedCsvReport);
        }
    }

    private void handleProcessingFailure(ClaimedReportJob claimedReportJob, Instant now, Exception ex) {
        var errorCode = "report_processing_error";
        var errorMessage = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
        if (claimedReportJob.attempt() >= reportProperties.getMaxAttempts()) {
            reportLifecycleDao.markFailed(claimedReportJob.id(), now, now, errorCode, errorMessage);
            return;
        }
        reportLifecycleDao.rescheduleForRetry(claimedReportJob.id(), now.plus(RETRY_BACKOFF), errorCode, errorMessage);
    }

    private ReportFileMetadata buildFileMetadata(
            String fileId,
            GeneratedCsvReport generatedCsvReport
    ) {
        return new ReportFileMetadata(
                fileId,
                generatedCsvReport.fileName(),
                generatedCsvReport.contentType(),
                generatedCsvReport.sizeBytes(),
                generatedCsvReport.md5(),
                generatedCsvReport.sha256(),
                "file-storage",
                fileId
        );
    }

    private void deleteStagedFile(GeneratedCsvReport generatedCsvReport) {
        if (generatedCsvReport == null) {
            return;
        }
        try {
            Files.deleteIfExists(generatedCsvReport.contentPath());
        } catch (IOException ignored) {
            // Best-effort cleanup for staged files after upload/publication.
        }
    }
}
