package dev.vality.ccreporter.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.Report;
import dev.vality.ccreporter.ReportStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class ReportLifecycleConcurrencyIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void concurrentWorkersDoNotDoubleProcessSinglePendingReport() throws Exception {
        insertPaymentRow(
                "invoice-concurrency-1",
                "payment-concurrency-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        long reportId = reportingHandler.createReport(createPaymentsReportRequest("concurrency-single-1"));

        CountDownLatch uploadEntered = new CountDownLatch(1);
        CountDownLatch releaseUpload = new CountDownLatch(1);
        stubFileStorageClient.blockUploads(uploadEntered, releaseUpload);

        ConcurrentWorkers workers = startWorkers(
                2,
                () -> reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"))
        );
        List<Boolean> results;
        try {
            assertThat(uploadEntered.await(5, TimeUnit.SECONDS)).isTrue();
            releaseUpload.countDown();
            results = awaitResults(workers.futures());
        } finally {
            workers.shutdown();
        }

        assertThat(results).containsExactlyInAnyOrder(true, false);
        assertThat(countRows("SELECT count(*) FROM ccr.report_file WHERE report_id = ?", reportId)).isEqualTo(1);
        assertThat(readAttempt(reportId)).isEqualTo(1);

        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(report.getFile().getFileId()).isNotBlank();
    }

    @Test
    void concurrentWorkersClaimDifferentPendingReportsWithoutDuplicates() throws Exception {
        insertPaymentRow(
                "invoice-concurrency-2",
                "payment-concurrency-2",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        insertPaymentRow(
                "invoice-concurrency-3",
                "payment-concurrency-3",
                Instant.parse("2026-01-01T10:01:00Z"),
                Instant.parse("2026-01-01T11:01:00Z")
        );
        long firstReportId = reportingHandler.createReport(createPaymentsReportRequest("concurrency-multi-1"));
        long secondReportId = reportingHandler.createReport(createPaymentsReportRequest("concurrency-multi-2"));

        CountDownLatch uploadEntered = new CountDownLatch(2);
        CountDownLatch releaseUpload = new CountDownLatch(1);
        stubFileStorageClient.blockUploads(uploadEntered, releaseUpload);

        ConcurrentWorkers workers = startWorkers(
                2,
                () -> reportLifecycleService.processNextPendingReport(Instant.parse("2026-01-01T12:00:00Z"))
        );
        List<Boolean> results;
        try {
            assertThat(uploadEntered.await(5, TimeUnit.SECONDS)).isTrue();
            releaseUpload.countDown();
            results = awaitResults(workers.futures());
        } finally {
            workers.shutdown();
        }

        assertThat(results).containsExactlyInAnyOrder(true, true);
        assertThat(countRows("SELECT count(*) FROM ccr.report_file WHERE report_id IN (?, ?)", firstReportId, secondReportId))
                .isEqualTo(2);
        assertThat(readAttempt(firstReportId)).isEqualTo(1);
        assertThat(readAttempt(secondReportId)).isEqualTo(1);
        assertCreatedReport(firstReportId);
        assertCreatedReport(secondReportId);
        assertThat(countRows("SELECT count(DISTINCT file_id) FROM ccr.report_file WHERE report_id IN (?, ?)", firstReportId, secondReportId))
                .isEqualTo(2);
    }

    private ConcurrentWorkers startWorkers(int workers, Callable<Boolean> task) {
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            futures.add(executor.submit(() -> {
                startLatch.await(5, TimeUnit.SECONDS);
                return task.call();
            }));
        }
        startLatch.countDown();
        return new ConcurrentWorkers(executor, futures);
    }

    private List<Boolean> awaitResults(List<Future<Boolean>> futures)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<Boolean> results = new ArrayList<>(futures.size());
        for (Future<Boolean> future : futures) {
            results.add(future.get(10, TimeUnit.SECONDS));
        }
        return results;
    }

    private int countRows(String sql, Object... args) {
        return jdbcTemplate.queryForObject(sql, Integer.class, args);
    }

    private int readAttempt(long reportId) {
        return jdbcTemplate.queryForObject(
                "SELECT attempt FROM ccr.report_job WHERE id = ?",
                Integer.class,
                reportId
        );
    }

    private void assertCreatedReport(long reportId) throws Exception {
        Report report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(2L);
        assertThat(report.getFile().getFileId()).isNotBlank();
    }

    private record ConcurrentWorkers(ExecutorService executor, List<Future<Boolean>> futures) {

        void shutdown() throws InterruptedException {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
