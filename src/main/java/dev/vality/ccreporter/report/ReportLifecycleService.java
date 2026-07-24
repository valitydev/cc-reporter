package dev.vality.ccreporter.report;

import dev.vality.ccreporter.config.properties.ReportProperties;
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
import java.util.ArrayList;
import java.util.concurrent.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReportLifecycleService {

    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(30);

    private final ReportLifecycleDao reportLifecycleDao;
    private final ReportCsvService reportCsvService;
    private final FileStorageService fileStorageService;
    private final ReportProperties reportProperties;
    private final ExecutorService reportWorkerExecutor;

    public void runLifecycleTick() {
        var now = Instant.now();
        timeoutStaleProcessingReports(now);
        expireReadyReports(now);
        while (!Thread.currentThread().isInterrupted()
                && processPendingBatch(Instant.now()) == reportProperties.getWorkerConcurrency()) {
            // Drain ready reports in bounded parallel batches.
        }
    }

    public int timeoutStaleProcessingReports(Instant now) {
        var staleBefore = now.minusMillis(reportProperties.getProcessingTimeoutMs());
        var timedOutReports = reportLifecycleDao.timeoutStaleProcessingReports(staleBefore, now);
        if (timedOutReports > 0) {
            log.warn("Timed out {} stale processing report(s)", timedOutReports);
        }
        return timedOutReports;
    }

    public int expireReadyReports(Instant now) {
        var expiredReports = reportLifecycleDao.expireReports(now);
        if (expiredReports > 0) {
            log.info("Expired {} report(s)", expiredReports);
        }
        return expiredReports;
    }

    public boolean processNextPendingReport(Instant now) {
        var reportTask = reportLifecycleDao.claimNextPendingReport(now);
        if (reportTask.isEmpty()) {
            return false;
        }
        var runningReport = startReport(reportTask.get());
        return runningReport != null && awaitReport(runningReport);
    }

    private int processPendingBatch(Instant now) {
        var runningReports = new ArrayList<RunningReport>(reportProperties.getWorkerConcurrency());
        for (int worker = 0; worker < reportProperties.getWorkerConcurrency(); worker++) {
            var reportTask = reportLifecycleDao.claimNextPendingReport(now);
            if (reportTask.isEmpty()) {
                break;
            }
            var runningReport = startReport(reportTask.get());
            if (runningReport == null) {
                break;
            }
            runningReports.add(runningReport);
        }
        for (int reportIndex = 0; reportIndex < runningReports.size(); reportIndex++) {
            if (!awaitReport(runningReports.get(reportIndex))) {
                cancelRemainingReports(runningReports, reportIndex + 1);
                break;
            }
        }
        return runningReports.size();
    }

    private RunningReport startReport(ReportTask reportTask) {
        try {
            var processing = reportWorkerExecutor.submit(() -> processReportTask(reportTask));
            var deadlineNanos = System.nanoTime() +
                    TimeUnit.MILLISECONDS.toNanos(reportProperties.getProcessingTimeoutMs());
            log.info("Started report {} processing attempt {}", reportTask.id(), reportTask.attempt());
            return new RunningReport(reportTask, processing, deadlineNanos);
        } catch (RejectedExecutionException ex) {
            handleProcessingFailure(reportTask, Instant.now(), ex);
            return null;
        }
    }

    private boolean awaitReport(RunningReport runningReport) {
        var reportTask = runningReport.reportTask();
        var processing = runningReport.processing();
        try {
            var remainingNanos = runningReport.deadlineNanos() - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new TimeoutException("Report processing deadline elapsed");
            }
            processing.get(remainingNanos, TimeUnit.NANOSECONDS);
            return true;
        } catch (TimeoutException ex) {
            timeoutReport(reportTask, processing, "maximum processing time exceeded");
            return true;
        } catch (CancellationException ex) {
            timeoutReport(reportTask, processing, "worker execution was canceled");
            return false;
        } catch (InterruptedException ex) {
            timeoutReport(reportTask, processing, "scheduler thread was interrupted");
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException ex) {
            handleProcessingFailure(reportTask, Instant.now(), ex.getCause());
            return true;
        }
    }

    private void cancelRemainingReports(ArrayList<RunningReport> runningReports, int firstReportIndex) {
        for (int reportIndex = firstReportIndex; reportIndex < runningReports.size(); reportIndex++) {
            var runningReport = runningReports.get(reportIndex);
            timeoutReport(
                    runningReport.reportTask(),
                    runningReport.processing(),
                    "scheduler stopped while processing batch"
            );
        }
    }

    private void processReportTask(ReportTask reportTask) {
        GeneratedCsvReport generatedReport = null;
        try {
            generatedReport = reportCsvService.generate(reportTask);
            throwIfInterrupted();
            var expiresAt = Instant.now().plusSeconds(reportProperties.getExpirationSec());
            var fileId = fileStorageService.storeFile(
                    generatedReport.fileName(),
                    generatedReport.contentType(),
                    generatedReport.contentPath(),
                    expiresAt
            );
            throwIfInterrupted();
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
            } else {
                log.info(
                        "Completed report {} with {} row(s)",
                        reportTask.id(),
                        generatedReport.rowsCount()
                );
            }
        } finally {
            deleteStagedFile(generatedReport);
        }
    }

    private void timeoutReport(ReportTask reportTask, Future<?> processing, String reason) {
        processing.cancel(true);
        var finishedAt = Instant.now();
        try {
            var timedOut = reportLifecycleDao.markTimedOut(reportTask.id(), finishedAt);
            if (timedOut) {
                log.warn("Report {} timed out: {}", reportTask.id(), reason);
            } else {
                log.info("Report {} changed state before timeout transition", reportTask.id());
            }
        } catch (RuntimeException ex) {
            log.error(
                    "Failed to mark report {} as timed out after worker cancellation",
                    reportTask.id(),
                    ex
            );
        }
    }

    private void handleProcessingFailure(ReportTask reportTask, Instant now, Throwable ex) {
        var errorCode = "report_processing_error";
        var errorMessage = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
        if (reportTask.attempt() >= reportProperties.getMaxAttempts()) {
            var failed = reportLifecycleDao.markFailed(reportTask.id(), now, errorCode, errorMessage);
            if (failed) {
                log.error(
                        "Report {} failed after {} attempt(s): {}",
                        reportTask.id(),
                        reportTask.attempt(),
                        errorMessage,
                        ex
                );
            } else {
                log.info("Report {} changed state before failed transition", reportTask.id());
            }
        } else {
            var nextAttemptAt = now.plus(RETRY_BACKOFF);
            var rescheduled = reportLifecycleDao.rescheduleForRetry(
                    reportTask.id(),
                    nextAttemptAt,
                    errorCode,
                    errorMessage
            );
            if (rescheduled) {
                log.warn(
                        "Report {} attempt {} failed; next attempt at {}: {}",
                        reportTask.id(),
                        reportTask.attempt(),
                        nextAttemptAt,
                        errorMessage,
                        ex
                );
            } else {
                log.info("Report {} changed state before retry transition", reportTask.id());
            }
        }
    }

    private void throwIfInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Report processing was interrupted");
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

    private record RunningReport(
            ReportTask reportTask,
            Future<?> processing,
            long deadlineNanos
    ) {
    }
}
