package dev.vality.ccreporter.report;

import dev.vality.ccreporter.config.properties.ReportProperties;
import dev.vality.ccreporter.config.properties.ReportSchedulerProperties;
import dev.vality.ccreporter.dao.ClaimedReportJob;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.storage.FileStorageService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Service
public class ReportLifecycleService {

    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(30);

    private final ReportLifecycleDao reportLifecycleDao;
    private final ReportCsvService reportCsvService;
    private final FileStorageService fileStorageService;
    private final ReportProperties reportProperties;
    private final ReportSchedulerProperties reportSchedulerProperties;
    private final TransactionTemplate transactionTemplate;

    public ReportLifecycleService(
            ReportLifecycleDao reportLifecycleDao,
            ReportCsvService reportCsvService,
            FileStorageService fileStorageService,
            ReportProperties reportProperties,
            ReportSchedulerProperties reportSchedulerProperties,
            PlatformTransactionManager transactionManager
    ) {
        this.reportLifecycleDao = reportLifecycleDao;
        this.reportCsvService = reportCsvService;
        this.fileStorageService = fileStorageService;
        this.reportProperties = reportProperties;
        this.reportSchedulerProperties = reportSchedulerProperties;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public boolean processNextPendingReport() {
        return processNextPendingReport(Instant.now());
    }

    public boolean processNextPendingReport(Instant now) {
        Optional<ClaimedReportJob> claimedReportJob = reportLifecycleDao.claimNextPendingReport(now);
        if (claimedReportJob.isEmpty()) {
            return false;
        }
        processClaimedReport(claimedReportJob.get(), now);
        return true;
    }

    public int timeoutStaleProcessingReports() {
        return timeoutStaleProcessingReports(Instant.now());
    }

    public int timeoutStaleProcessingReports(Instant now) {
        Instant staleBefore = now.minusMillis(reportSchedulerProperties.getStaleProcessingTimeoutMs());
        return reportLifecycleDao.timeoutStaleProcessingReports(staleBefore, now);
    }

    public int expireReadyReports() {
        return expireReadyReports(Instant.now());
    }

    public int expireReadyReports(Instant now) {
        return reportLifecycleDao.expireReports(now);
    }

    private void processClaimedReport(ClaimedReportJob claimedReportJob, Instant processingTime) {
        GeneratedCsvReport generatedCsvReport = null;
        try {
            generatedCsvReport = reportCsvService.generate(claimedReportJob);
            Instant expiresAt = processingTime.plusSeconds(reportProperties.getExpirationSec());
            String fileId = fileStorageService.storeFile(
                    generatedCsvReport.fileName(),
                    generatedCsvReport.contentType(),
                    generatedCsvReport.contentPath(),
                    expiresAt
            );
            Instant finishedAt = Instant.now();
            ReportFileMetadata fileMetadata = buildFileMetadata(fileId, generatedCsvReport);
            GeneratedCsvReport completedReport = generatedCsvReport;
            transactionTemplate.executeWithoutResult(status -> {
                boolean published = reportLifecycleDao.publishFileRecord(
                        claimedReportJob.id(),
                        fileMetadata,
                        finishedAt
                );
                boolean markedCreated = reportLifecycleDao.markCreated(
                        claimedReportJob.id(),
                        completedReport.dataSnapshotFixedAt(),
                        finishedAt,
                        expiresAt,
                        completedReport.rowsCount()
                );
                if (!published || !markedCreated) {
                    status.setRollbackOnly();
                    throw new IllegalStateException("Failed to publish created report " + claimedReportJob.id());
                }
            });
        } catch (Exception ex) {
            handleProcessingFailure(claimedReportJob, processingTime, ex);
        } finally {
            deleteStagedFile(generatedCsvReport);
        }
    }

    private void handleProcessingFailure(ClaimedReportJob claimedReportJob, Instant now, Exception ex) {
        String errorCode = "report_processing_error";
        String errorMessage = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
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
