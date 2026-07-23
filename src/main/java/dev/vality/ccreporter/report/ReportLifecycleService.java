package dev.vality.ccreporter.report;

import dev.vality.ccreporter.config.properties.ReportProperties;
import dev.vality.ccreporter.config.properties.ReportSchedulerProperties;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.model.GeneratedCsvReport;
import dev.vality.ccreporter.model.ReportTask;
import dev.vality.ccreporter.storage.FileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReportLifecycleService {

    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(30);

    private final ReportLifecycleDao reportLifecycleDao;
    private final ReportCsvService reportCsvService;
    private final FileStorageService fileStorageService;
    private final ReportProperties reportProperties;
    private final ReportSchedulerProperties reportSchedulerProperties;

    public void runLifecycleTick() {
        var now = Instant.now();
        timeoutStaleProcessingReports(now);
        expireReadyReports(now);
        while (processNextPendingReport(Instant.now())) {
            // Drain all reports that are ready now; failed reports are rescheduled into the future.
        }
    }

    public int timeoutStaleProcessingReports(Instant now) {
        var staleBefore = now.minusMillis(reportSchedulerProperties.getStaleProcessingTimeoutMs());
        return reportLifecycleDao.timeoutStaleProcessingReports(staleBefore, now);
    }

    public int expireReadyReports(Instant now) {
        return reportLifecycleDao.expireReports(now);
    }

    public boolean processNextPendingReport(Instant now) {
        return reportLifecycleDao.claimNextPendingReport(now)
                .map(reportTask -> {
                    processReportTask(reportTask, now);
                    return true;
                })
                .orElse(false);
    }

    private void processReportTask(ReportTask reportTask, Instant processingTime) {
        GeneratedCsvReport generatedReport = null;
        try {
            generatedReport = reportCsvService.generate(reportTask);
            var expiresAt = Instant.now().plusSeconds(reportProperties.getExpirationSec());
            var fileId = fileStorageService.storeFile(
                    generatedReport.fileName(),
                    generatedReport.contentType(),
                    generatedReport.contentPath(),
                    expiresAt
            );
            var reportFile = buildReportFile(fileId, generatedReport);
            var finishedAt = Instant.now();
            var completed = reportLifecycleDao.completeReport(
                    reportTask.id(),
                    reportFile,
                    generatedReport.dataSnapshotFixedAt(),
                    finishedAt,
                    expiresAt,
                    generatedReport.rowsCount()
            );
            if (!completed) {
                log.info(
                        "Report {} changed state while it was being generated; uploaded file will expire",
                        reportTask.id()
                );
            }
        } catch (Exception ex) {
            handleProcessingFailure(reportTask, processingTime, ex);
        } finally {
            deleteStagedFile(generatedReport);
        }
    }

    private void handleProcessingFailure(ReportTask reportTask, Instant now, Exception ex) {
        var errorCode = "report_processing_error";
        var errorMessage = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
        if (reportTask.attempt() >= reportProperties.getMaxAttempts()) {
            reportLifecycleDao.markFailed(reportTask.id(), now, errorCode, errorMessage);
        } else {
            reportLifecycleDao.rescheduleForRetry(
                    reportTask.id(),
                    now.plus(RETRY_BACKOFF),
                    errorCode,
                    errorMessage
            );
        }
    }

    private ReportFile buildReportFile(String fileId, GeneratedCsvReport generatedReport) {
        return new ReportFile()
                .setFileId(fileId)
                .setFileType(dev.vality.ccreporter.domain.enums.FileType.csv)
                .setFilename(generatedReport.fileName())
                .setContentType(generatedReport.contentType())
                .setSizeBytes(generatedReport.sizeBytes())
                .setMd5(generatedReport.md5())
                .setSha256(generatedReport.sha256());
    }

    private void deleteStagedFile(GeneratedCsvReport generatedReport) {
        if (generatedReport == null) {
            return;
        }
        try {
            Files.deleteIfExists(generatedReport.contentPath());
        } catch (IOException ex) {
            log.warn("Failed to delete staged report file {}", generatedReport.contentPath(), ex);
        }
    }
}
