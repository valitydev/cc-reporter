package dev.vality.ccreporter.service;

import dev.vality.ccreporter.config.ReportProperties;
import dev.vality.ccreporter.config.ReportSchedulerProperties;
import dev.vality.ccreporter.dao.ClaimedReportJob;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class ReportLifecycleService {

    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(30);

    private final ReportLifecycleDao reportLifecycleDao;
    private final ReportCsvService reportCsvService;
    private final FileStorageClient fileStorageClient;
    private final ReportProperties reportProperties;
    private final ReportSchedulerProperties reportSchedulerProperties;
    private final TransactionTemplate transactionTemplate;

    public ReportLifecycleService(
            ReportLifecycleDao reportLifecycleDao,
            ReportCsvService reportCsvService,
            FileStorageClient fileStorageClient,
            ReportProperties reportProperties,
            ReportSchedulerProperties reportSchedulerProperties,
            PlatformTransactionManager transactionManager
    ) {
        this.reportLifecycleDao = reportLifecycleDao;
        this.reportCsvService = reportCsvService;
        this.fileStorageClient = fileStorageClient;
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
        try {
            GeneratedCsvReport generatedCsvReport = reportCsvService.generate(claimedReportJob);
            Instant expiresAt = processingTime.plusSeconds(reportProperties.getExpirationSec());
            String fileId = fileStorageClient.storeFile(
                    generatedCsvReport.fileName(),
                    generatedCsvReport.contentType(),
                    generatedCsvReport.content(),
                    expiresAt
            );
            Instant finishedAt = Instant.now();
            ReportFileMetadata fileMetadata = buildFileMetadata(
                    fileId,
                    generatedCsvReport.fileName(),
                    generatedCsvReport.contentType(),
                    generatedCsvReport.content()
            );
            transactionTemplate.executeWithoutResult(status -> {
                boolean published = reportLifecycleDao.publishFileRecord(
                        claimedReportJob.id(),
                        fileMetadata,
                        finishedAt
                );
                boolean markedCreated = reportLifecycleDao.markCreated(
                        claimedReportJob.id(),
                        generatedCsvReport.dataSnapshotFixedAt(),
                        finishedAt,
                        expiresAt,
                        generatedCsvReport.rowsCount()
                );
                if (!published || !markedCreated) {
                    status.setRollbackOnly();
                    throw new IllegalStateException("Failed to publish created report " + claimedReportJob.id());
                }
            });
        } catch (Exception ex) {
            handleProcessingFailure(claimedReportJob, processingTime, ex);
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
            String fileName,
            String contentType,
            byte[] content
    ) {
        return new ReportFileMetadata(
                fileId,
                fileName,
                contentType,
                content.length,
                digest("MD5", content),
                digest("SHA-256", content),
                "file-storage",
                fileId
        );
    }

    private String digest(String algorithm, byte[] content) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
            return HexFormat.of().formatHex(messageDigest.digest(content));
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to calculate " + algorithm + " digest", ex);
        }
    }
}
