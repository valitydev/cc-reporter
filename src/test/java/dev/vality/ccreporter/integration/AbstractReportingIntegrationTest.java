package dev.vality.ccreporter.integration;

import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.ReportingSrv;
import dev.vality.ccreporter.TimeRange;
import dev.vality.ccreporter.service.FileStorageClient;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
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
                "ccr.storage.file-storage.url=http://localhost:8022/file-storage"
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

    private static EmbeddedPostgres startPostgres() {
        try {
            return EmbeddedPostgres.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start embedded PostgreSQL for test", ex);
        }
    }

    static class StubFileStorageClient implements FileStorageClient {

        private final AtomicReference<String> lastFileId = new AtomicReference<>();
        private final AtomicReference<Instant> lastExpiresAt = new AtomicReference<>();

        @Override
        public String generateDownloadUrl(String fileId, Instant expiresAt) {
            lastFileId.set(fileId);
            lastExpiresAt.set(expiresAt);
            return "https://download.example/" + fileId;
        }

        void reset() {
            lastFileId.set(null);
            lastExpiresAt.set(null);
        }

        String getLastFileId() {
            return lastFileId.get();
        }

        Instant getLastExpiresAt() {
            return lastExpiresAt.get();
        }
    }
}
