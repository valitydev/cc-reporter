package dev.vality.ccreporter.report;

import dev.vality.ccreporter.config.properties.ReportProperties;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.domain.enums.ReportType;
import dev.vality.ccreporter.model.ReportTask;
import dev.vality.ccreporter.storage.FileStorageService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ReportLifecycleServiceTest {

    @Test
    void hardTimeoutInterruptsWorkerAndMarksReportTimedOut() throws Exception {
        var reportLifecycleDao = mock(ReportLifecycleDao.class);
        var reportCsvService = mock(ReportCsvService.class);
        var fileStorageService = mock(FileStorageService.class);
        var reportProperties = reportProperties(50);
        var reportTask = reportTask(1);
        var workerStarted = new CountDownLatch(1);
        var workerInterrupted = new CountDownLatch(1);
        var reportWorkerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            when(reportLifecycleDao.claimNextPendingReport(any())).thenReturn(Optional.of(reportTask));
            when(reportLifecycleDao.markTimedOut(eq(reportTask.id()), any())).thenAnswer(invocation -> {
                assertThat(workerInterrupted.await(1, TimeUnit.SECONDS)).isTrue();
                return true;
            });
            when(reportCsvService.generate(reportTask)).thenAnswer(invocation -> {
                workerStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                    throw new AssertionError("Worker must be interrupted");
                } catch (InterruptedException ex) {
                    workerInterrupted.countDown();
                    Thread.currentThread().interrupt();
                    throw new CancellationException("interrupted");
                }
            });

            var service = new ReportLifecycleService(
                    reportLifecycleDao,
                    reportCsvService,
                    fileStorageService,
                    reportProperties,
                    reportWorkerExecutor
            );

            var processed = service.processNextPendingReport(Instant.now());

            assertThat(processed).isTrue();
            assertThat(workerStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(workerInterrupted.await(1, TimeUnit.SECONDS)).isTrue();
            verify(reportLifecycleDao).markTimedOut(eq(reportTask.id()), any());
            verifyNoInteractions(fileStorageService);
        } finally {
            reportWorkerExecutor.shutdownNow();
        }
    }

    @Test
    void retryIsScheduledFromFailureTime() {
        var reportLifecycleDao = mock(ReportLifecycleDao.class);
        var reportCsvService = mock(ReportCsvService.class);
        var fileStorageService = mock(FileStorageService.class);
        var reportProperties = reportProperties(1_000);
        var reportTask = reportTask(1);
        var reportWorkerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            when(reportLifecycleDao.claimNextPendingReport(any())).thenReturn(Optional.of(reportTask));
            when(reportLifecycleDao.rescheduleForRetry(anyLong(), any(), anyString(), anyString())).thenReturn(true);
            when(reportCsvService.generate(reportTask)).thenThrow(new IllegalStateException("generation failed"));
            var service = new ReportLifecycleService(
                    reportLifecycleDao,
                    reportCsvService,
                    fileStorageService,
                    reportProperties,
                    reportWorkerExecutor
            );
            var beforeFailure = Instant.now();

            service.processNextPendingReport(Instant.parse("2026-01-01T00:00:00Z"));

            var nextAttemptCaptor = ArgumentCaptor.forClass(Instant.class);
            verify(reportLifecycleDao).rescheduleForRetry(
                    eq(reportTask.id()),
                    nextAttemptCaptor.capture(),
                    eq("report_processing_error"),
                    eq("generation failed")
            );
            assertThat(nextAttemptCaptor.getValue())
                    .isBetween(
                            beforeFailure.plus(Duration.ofSeconds(30)),
                            Instant.now().plus(Duration.ofSeconds(30))
                    );
        } finally {
            reportWorkerExecutor.shutdownNow();
        }
    }

    private ReportProperties reportProperties(long processingTimeoutMs) {
        var properties = new ReportProperties();
        properties.setMaxAttempts(2);
        properties.setWorkerConcurrency(2);
        properties.setProcessingTimeoutMs(processingTimeoutMs);
        properties.setExpirationSec(600);
        return properties;
    }

    private ReportTask reportTask(int attempt) {
        return new ReportTask(42L, ReportType.payments, "{}", "UTC", attempt);
    }
}
