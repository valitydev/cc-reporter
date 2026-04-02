package dev.vality.ccreporter.report;

import dev.vality.ccreporter.config.properties.ReportProperties;
import dev.vality.ccreporter.config.properties.ReportSchedulerProperties;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.domain.tables.pojos.ReportJob;
import dev.vality.ccreporter.model.GeneratedCsvReport;
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
        var reportJob = reportLifecycleDao.claimNextPendingReport(now);
        if (reportJob.isEmpty()) {
            return false;
        }
        processReportJob(reportJob.get(), now);
        return true;
    }

    private void processReportJob(ReportJob reportJob, Instant processingTime) {
        var generatedCsvReport = (GeneratedCsvReport) null;
        try {
            generatedCsvReport = reportCsvService.generate(reportJob);
            var expiresAt = processingTime.plusSeconds(reportProperties.getExpirationSec());
            var fileId = fileStorageService.storeFile(
                    generatedCsvReport.fileName(),
                    generatedCsvReport.contentType(),
                    generatedCsvReport.contentPath(),
                    expiresAt
            );
            var finishedAt = Instant.now();
            var reportFile = buildReportFile(fileId, generatedCsvReport);
            reportLifecycleTransactionService.publishCompletedReport(
                    reportJob.getId(),
                    reportFile,
                    finishedAt,
                    expiresAt,
                    generatedCsvReport
            );
        } catch (Exception ex) {
            handleProcessingFailure(reportJob, processingTime, ex);
        } finally {
            deleteStagedFile(generatedCsvReport);
        }
    }

    private void handleProcessingFailure(ReportJob reportJob, Instant now, Exception ex) {
        var errorCode = "report_processing_error";
        var errorMessage = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
        if (reportJob.getAttempt() >= reportProperties.getMaxAttempts()) {
            reportLifecycleDao.markFailed(reportJob.getId(), now, now, errorCode, errorMessage);
            return;
        }
        reportLifecycleDao.rescheduleForRetry(reportJob.getId(), now.plus(RETRY_BACKOFF), errorCode, errorMessage);
    }

    private ReportFile buildReportFile(
            String fileId,
            GeneratedCsvReport generatedCsvReport
    ) {
        return new ReportFile()
                .setFileId(fileId)
                .setFileType(dev.vality.ccreporter.domain.enums.FileType.csv)
                .setBucket("file-storage")
                .setObjectKey(fileId)
                .setFilename(generatedCsvReport.fileName())
                .setContentType(generatedCsvReport.contentType())
                .setSizeBytes(generatedCsvReport.sizeBytes())
                .setMd5(generatedCsvReport.md5())
                .setSha256(generatedCsvReport.sha256());
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
