package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.ReportingSrv;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.TimeRange;
import dev.vality.ccreporter.WithdrawalsQuery;
import dev.vality.ccreporter.service.FileStorageClient;
import dev.vality.ccreporter.service.ReportLifecycleService;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.port=0",
                "management.server.port=0",
                "ccr.storage.file-storage.url=http://localhost:8022/file-storage",
                "ccr.report.max-attempts=2",
                "ccr.report.expiration-sec=600",
                "ccr.scheduler.stale-processing-timeout-ms=60000"
        }
)
@Import(ReportingIntegrationTestConfig.class)
abstract class AbstractReportingIntegrationTest {

    private static final EmbeddedPostgres EMBEDDED_POSTGRES = startPostgres();
    private static final String JDBC_URL = EMBEDDED_POSTGRES.getJdbcUrl("postgres", "postgres");

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected ReportingSrv.Iface reportingHandler;

    @Autowired
    protected StubFileStorageClient stubFileStorageClient;

    @Autowired
    protected ReportLifecycleDao reportLifecycleDao;

    @Autowired
    protected ReportLifecycleService reportLifecycleService;

    @DynamicPropertySource
    static void registerDatasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> JDBC_URL);
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "");
    }

    @BeforeEach
    void setUpBaseState() {
        jdbcTemplate.update("DELETE FROM ccr.report_file");
        jdbcTemplate.update("DELETE FROM ccr.report_job");
        jdbcTemplate.update("DELETE FROM ccr.payment_txn_current");
        jdbcTemplate.update("DELETE FROM ccr.withdrawal_txn_current");
        jdbcTemplate.update("DELETE FROM ccr.withdrawal_session_binding_current");
        stubFileStorageClient.reset();
        bindCaller("user-1");
    }

    @AfterEach
    void tearDownRequestContext() {
        RequestContextHolder.resetRequestAttributes();
    }

    protected void bindCaller(String callerId) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("X-User-Id", callerId);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    protected CreateReportRequest createPaymentsReportRequest(String idempotencyKey) {
        TimeRange timeRange = new TimeRange(
                Instant.parse("2026-01-01T00:00:00Z").toString(),
                Instant.parse("2026-01-02T00:00:00Z").toString()
        );
        PaymentsQuery paymentsQuery = new PaymentsQuery();
        paymentsQuery.setTimeRange(timeRange);
        ReportQuery reportQuery = new ReportQuery();
        reportQuery.setPayments(paymentsQuery);

        CreateReportRequest request = new CreateReportRequest();
        request.setReportType(ReportType.payments);
        request.setFileType(FileType.csv);
        request.setQuery(reportQuery);
        request.setIdempotencyKey(idempotencyKey);
        return request;
    }

    protected CreateReportRequest createWithdrawalsReportRequest(String idempotencyKey) {
        TimeRange timeRange = new TimeRange(
                Instant.parse("2026-01-01T00:00:00Z").toString(),
                Instant.parse("2026-01-02T00:00:00Z").toString()
        );
        WithdrawalsQuery withdrawalsQuery = new WithdrawalsQuery();
        withdrawalsQuery.setTimeRange(timeRange);
        ReportQuery reportQuery = new ReportQuery();
        reportQuery.setWithdrawals(withdrawalsQuery);

        CreateReportRequest request = new CreateReportRequest();
        request.setReportType(ReportType.withdrawals);
        request.setFileType(FileType.csv);
        request.setQuery(reportQuery);
        request.setIdempotencyKey(idempotencyKey);
        return request;
    }

    protected void markReportProcessing(long reportId, Instant startedAt, Instant snapshotFixedAt) {
        jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = 'processing',
                    started_at = ?,
                    data_snapshot_fixed_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                Timestamp.from(startedAt),
                Timestamp.from(snapshotFixedAt),
                Timestamp.from(snapshotFixedAt),
                reportId
        );
    }

    protected void markReportCreated(
            long reportId,
            Instant startedAt,
            Instant snapshotFixedAt,
            Instant finishedAt,
            Instant expiresAt,
            long rowsCount
    ) {
        jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = 'created',
                    started_at = ?,
                    data_snapshot_fixed_at = ?,
                    finished_at = ?,
                    expires_at = ?,
                    rows_count = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                Timestamp.from(startedAt),
                Timestamp.from(snapshotFixedAt),
                Timestamp.from(finishedAt),
                Timestamp.from(expiresAt),
                rowsCount,
                Timestamp.from(finishedAt),
                reportId
        );
    }

    protected void markReportFailed(
            long reportId,
            Instant startedAt,
            Instant snapshotFixedAt,
            Instant finishedAt,
            String errorCode,
            String errorMessage
    ) {
        jdbcTemplate.update(
                """
                UPDATE ccr.report_job
                SET status = 'failed',
                    started_at = ?,
                    data_snapshot_fixed_at = ?,
                    finished_at = ?,
                    error_code = ?,
                    error_message = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                Timestamp.from(startedAt),
                Timestamp.from(snapshotFixedAt),
                Timestamp.from(finishedAt),
                errorCode,
                errorMessage,
                Timestamp.from(finishedAt),
                reportId
        );
    }

    protected void attachCsvFile(long reportId, String fileId, Instant createdAt) {
        jdbcTemplate.update(
                """
                INSERT INTO ccr.report_file (
                    report_id,
                    file_id,
                    file_type,
                    bucket,
                    object_key,
                    filename,
                    content_type,
                    size_bytes,
                    md5,
                    sha256,
                    created_at
                )
                VALUES (?, ?, 'csv', ?, ?, ?, 'text/csv', ?, ?, ?, ?)
                """,
                reportId,
                fileId,
                "bucket-1",
                "object-1",
                "payments.csv",
                128L,
                "md5-value",
                "sha256-value",
                Timestamp.from(createdAt)
        );
    }

    protected void insertPaymentRow(String invoiceId, String paymentId, Instant createdAt, Instant finalizedAt) {
        jdbcTemplate.update(
                """
                INSERT INTO ccr.payment_txn_current (
                    invoice_id, payment_id, domain_event_id, domain_event_created_at, party_id, shop_id, shop_name,
                    created_at, finalized_at, status, provider_id, provider_name, terminal_id, terminal_name,
                    amount, fee, currency, trx_id, external_id, rrn, approval_code, payment_tool_type,
                    original_amount, original_currency, converted_amount, exchange_rate_internal,
                    provider_amount, provider_currency, shop_search, provider_search, terminal_search, trx_search
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                invoiceId,
                paymentId,
                1L,
                Timestamp.from(createdAt),
                "party-1",
                "shop-1",
                "Shop One",
                Timestamp.from(createdAt),
                finalizedAt == null ? null : Timestamp.from(finalizedAt),
                "captured",
                "provider-1",
                "Provider One",
                "terminal-1",
                "Terminal One",
                1000L,
                10L,
                "RUB",
                "trx-1",
                "external-1",
                "rrn-1",
                "approval-1",
                "bank_card",
                1100L,
                "USD",
                1000L,
                new BigDecimal("1.1000000000"),
                990L,
                "EUR",
                "shop one",
                "provider one",
                "terminal one",
                "trx-1"
        );
    }

    protected void insertWithdrawalRow(String withdrawalId, Instant createdAt, Instant finalizedAt) {
        jdbcTemplate.update(
                """
                INSERT INTO ccr.withdrawal_txn_current (
                    withdrawal_id, domain_event_id, domain_event_created_at, party_id, wallet_id, wallet_name,
                    destination_id, created_at, finalized_at, status, provider_id, provider_name, terminal_id,
                    terminal_name, amount, fee, currency, trx_id, external_id, error_code, error_reason,
                    error_sub_failure, original_amount, original_currency, converted_amount, exchange_rate_internal,
                    provider_amount, provider_currency, wallet_search, provider_search, terminal_search, trx_search
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                withdrawalId,
                1L,
                Timestamp.from(createdAt),
                "party-1",
                "wallet-1",
                "Wallet One",
                "destination-1",
                Timestamp.from(createdAt),
                finalizedAt == null ? null : Timestamp.from(finalizedAt),
                "succeeded",
                "provider-1",
                "Provider One",
                "terminal-1",
                "Terminal One",
                2000L,
                20L,
                "RUB",
                "trx-w-1",
                "external-w-1",
                null,
                null,
                null,
                2100L,
                "USD",
                2000L,
                new BigDecimal("1.0500000000"),
                1990L,
                "EUR",
                "wallet one",
                "provider one",
                "terminal one",
                "trx-w-1"
        );
    }

    private static EmbeddedPostgres startPostgres() {
        try {
            return EmbeddedPostgres.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start embedded PostgreSQL for test", ex);
        }
    }

    static class StubFileStorageClient implements FileStorageClient {

        private final AtomicInteger uploadSequence = new AtomicInteger(0);
        private final AtomicReference<String> lastFileId = new AtomicReference<>();
        private final AtomicReference<Instant> lastExpiresAt = new AtomicReference<>();
        private final Map<String, byte[]> storedContent = new ConcurrentHashMap<>();
        private volatile boolean failUploads;
        private volatile CountDownLatch uploadEnteredLatch;
        private volatile CountDownLatch releaseUploadLatch;

        @Override
        public String storeFile(String fileName, String contentType, byte[] content, Instant expiresAt) {
            if (failUploads) {
                throw new IllegalStateException("upload failed");
            }
            CountDownLatch enteredLatch = uploadEnteredLatch;
            if (enteredLatch != null) {
                enteredLatch.countDown();
            }
            CountDownLatch releaseLatch = releaseUploadLatch;
            if (releaseLatch != null) {
                try {
                    if (!releaseLatch.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to release stub upload");
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting to release stub upload", ex);
                }
            }
            String fileId = "stored-file-" + uploadSequence.incrementAndGet();
            storedContent.put(fileId, content);
            lastFileId.set(fileId);
            lastExpiresAt.set(expiresAt);
            return fileId;
        }

        @Override
        public String generateDownloadUrl(String fileId, Instant expiresAt) {
            lastFileId.set(fileId);
            lastExpiresAt.set(expiresAt);
            return "https://download.example/" + fileId;
        }

        void reset() {
            uploadSequence.set(0);
            lastFileId.set(null);
            lastExpiresAt.set(null);
            storedContent.clear();
            failUploads = false;
            uploadEnteredLatch = null;
            releaseUploadLatch = null;
        }

        String getLastFileId() {
            return lastFileId.get();
        }

        Instant getLastExpiresAt() {
            return lastExpiresAt.get();
        }

        byte[] getStoredContent(String fileId) {
            return storedContent.get(fileId);
        }

        void setFailUploads(boolean failUploads) {
            this.failUploads = failUploads;
        }

        void blockUploads(CountDownLatch uploadEnteredLatch, CountDownLatch releaseUploadLatch) {
            this.uploadEnteredLatch = uploadEnteredLatch;
            this.releaseUploadLatch = releaseUploadLatch;
        }
    }
}
