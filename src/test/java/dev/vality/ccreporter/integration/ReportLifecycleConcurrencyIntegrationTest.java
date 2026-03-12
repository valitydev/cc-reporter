package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import dev.vality.ccreporter.integration.fixture.CurrentStateTableFixtures;
import dev.vality.ccreporter.integration.fixture.ReportRequestFixtures;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет, что конкурирующие воркеры не разбирают один и тот же отчёт как попало и не ломают статусы.
 */
class ReportLifecycleConcurrencyIntegrationTest extends AbstractReportingIntegrationTest {

    @Test
    void concurrentWorkersDoNotDoubleProcessSinglePendingReport() throws Exception {
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-concurrency-1",
                "payment-concurrency-1",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        final var reportId = reportingHandler.createReport(ReportRequestFixtures.payments("concurrency-single-1"));

        var uploadEntered = new CountDownLatch(1);
        var releaseUpload = new CountDownLatch(1);
        stubFileStorageClient.blockUploads(uploadEntered, releaseUpload);

        var workers = startWorkers(
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

        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(1L);
        assertThat(report.getFile().getFileId()).isNotBlank();
    }

    @Test
    void concurrentWorkersClaimDifferentPendingReportsWithoutDuplicates() throws Exception {
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-concurrency-2",
                "payment-concurrency-2",
                Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z")
        );
        CurrentStateTableFixtures.insertPaymentRow(
                jdbcTemplate,
                "invoice-concurrency-3",
                "payment-concurrency-3",
                Instant.parse("2026-01-01T10:01:00Z"),
                Instant.parse("2026-01-01T11:01:00Z")
        );
        final var firstReportId = reportingHandler.createReport(ReportRequestFixtures.payments("concurrency-multi-1"));
        final var secondReportId =
                reportingHandler.createReport(ReportRequestFixtures.payments("concurrency-multi-2"));

        var uploadEntered = new CountDownLatch(2);
        var releaseUpload = new CountDownLatch(1);
        stubFileStorageClient.blockUploads(uploadEntered, releaseUpload);

        var workers = startWorkers(
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
        assertThat(countRows("SELECT count(*) FROM ccr.report_file WHERE report_id IN (?, ?)", firstReportId,
                secondReportId))
                .isEqualTo(2);
        assertThat(readAttempt(firstReportId)).isEqualTo(1);
        assertThat(readAttempt(secondReportId)).isEqualTo(1);
        assertCreatedReport(firstReportId);
        assertCreatedReport(secondReportId);
        assertThat(countRows("SELECT count(DISTINCT file_id) FROM ccr.report_file WHERE report_id IN (?, ?)",
                firstReportId, secondReportId))
                .isEqualTo(2);
    }

    private ConcurrentWorkers startWorkers(int workers, Callable<Boolean> task) {
        var executor = Executors.newFixedThreadPool(workers);
        var startLatch = new CountDownLatch(1);
        var futures = new ArrayList<Future<Boolean>>();
        for (int i = 0; i < workers; i++) {
            futures.add(executor.submit(() -> {
                assertThat(startLatch.await(5, TimeUnit.SECONDS)).isTrue();
                return task.call();
            }));
        }
        startLatch.countDown();
        return new ConcurrentWorkers(executor, futures);
    }

    private List<Boolean> awaitResults(List<Future<Boolean>> futures)
            throws InterruptedException, ExecutionException, TimeoutException {
        var results = new ArrayList<Boolean>(futures.size());
        for (Future<Boolean> future : futures) {
            results.add(future.get(10, TimeUnit.SECONDS));
        }
        return results;
    }

    private int countRows(String sql, Object... args) {
        return Objects.requireNonNull(jdbcTemplate.queryForObject(sql, Integer.class, args));
    }

    private int readAttempt(long reportId) {
        return Objects.requireNonNull(jdbcTemplate.queryForObject(
                "SELECT attempt FROM ccr.report_job WHERE id = ?",
                Integer.class,
                reportId
        ));
    }

    private void assertCreatedReport(long reportId) throws Exception {
        var report = reportingHandler.getReport(new GetReportRequest(reportId));
        assertThat(report.getStatus()).isEqualTo(ReportStatus.created);
        assertThat(report.getRowsCount()).isEqualTo(2L);
        assertThat(report.getFile().getFileId()).isNotBlank();
    }

    private record ConcurrentWorkers(ExecutorService executor, List<Future<Boolean>> futures) {

        void shutdown() throws InterruptedException {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
