package dev.vality.ccreporter.integration.base;

import dev.vality.ccreporter.ReportingSrv;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.integration.config.ReportingIntegrationTestConfig;
import dev.vality.ccreporter.report.ReportLifecycleService;
import dev.vality.ccreporter.storage.FileStorageService;
import dev.vality.woody.api.trace.TraceData;
import dev.vality.woody.api.trace.context.TraceContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Общая test-среда для integration-сценариев: база, Spring-контекст, caller в request scope и заглушка file storage.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.port=0",
                "management.server.port=0",
                "storage.file-storage.url=http://localhost:8022/file-storage",
                "report.max-attempts=2",
                "report.worker-concurrency=2",
                "report.expiration-sec=600",
                "report.processing-timeout-ms=60000"
        }
)
@Import(ReportingIntegrationTestConfig.class)
public abstract class AbstractReportingIntegrationTest {

    private Scope requestTraceScope;

    private static final EmbeddedPostgres EMBEDDED_POSTGRES = startPostgres();
    private static final String JDBC_URL = EMBEDDED_POSTGRES.getJdbcUrl("postgres", "postgres");

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected ReportingSrv.Iface reportingHandler;

    @Autowired
    protected StubFileStorageService stubFileStorageClient;

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
        jdbcTemplate.update("DELETE FROM ccr.report_audit_event");
        jdbcTemplate.update("DELETE FROM ccr.report_file");
        jdbcTemplate.update("DELETE FROM ccr.report_job");
        jdbcTemplate.update("DELETE FROM ccr.payment_txn_current");
        jdbcTemplate.update("DELETE FROM ccr.withdrawal_txn_current");
        jdbcTemplate.update("DELETE FROM ccr.withdrawal_session");
        jdbcTemplate.update("DELETE FROM ccr.shop_lookup");
        jdbcTemplate.update("DELETE FROM ccr.provider_lookup");
        jdbcTemplate.update("DELETE FROM ccr.terminal_lookup");
        jdbcTemplate.update("DELETE FROM ccr.wallet_lookup");
        stubFileStorageClient.reset();
        bindCaller("user-1");
    }

    @AfterEach
    void tearDownRequestContext() {
        if (requestTraceScope != null) {
            requestTraceScope.close();
            requestTraceScope = null;
        }
        TraceContext.setCurrentTraceData(null);
    }

    protected void bindCaller(String callerId) {
        bindTraceContext(null, callerId, callerId, callerId, null, null, null, null);
    }

    protected void bindCallerWithAuditMetadata(String callerId) {
        bindTraceContext(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00aa0ba902b70000-01",
                "user-id-42",
                "alice",
                "alice@example.com",
                "external",
                "req-123",
                "2026-01-05T10:15:30Z",
                "rojo=00f067aa0ba902b7"
        );
    }

    protected void bindCallerIdentity(String userId, String email) {
        bindTraceContext(null, userId, userId, email, "external", null, null, null);
    }

    private void bindTraceContext(
            String traceparent,
            String userId,
            String username,
            String email,
            String realm,
            String requestId,
            String requestDeadline,
            String tracestate
    ) {
        var traceData = new TraceData();
        traceData.getActiveSpan().getSpan().setTraceId("audit-trace-id");
        var metadata = traceData.getActiveSpan().getCustomMetadata();
        metadata.putValue("user-identity.id", userId);
        metadata.putValue("user-identity.username", username);
        metadata.putValue("user-identity.email", email);
        metadata.putValue("user-identity.realm", realm);
        metadata.putValue("user-identity.X-Request-ID", requestId);
        metadata.putValue("user-identity.X-Request-Deadline", requestDeadline);
        TraceContext.setCurrentTraceData(traceData);
        bindOpenTelemetryContext(traceparent, tracestate);
    }

    private void bindOpenTelemetryContext(String traceparent, String tracestate) {
        if (!StringUtils.hasText(traceparent)) {
            return;
        }
        var parts = traceparent.split("-");
        var traceStateBuilder = TraceState.builder();
        if (StringUtils.hasText(tracestate)) {
            for (var member : tracestate.split(",")) {
                var keyValue = member.trim().split("=", 2);
                traceStateBuilder.put(keyValue[0], keyValue[1]);
            }
        }
        var spanContext = SpanContext.createFromRemoteParent(
                parts[1],
                parts[2],
                TraceFlags.fromHex(parts[3], 0),
                traceStateBuilder.build()
        );
        requestTraceScope = Span.wrap(spanContext).makeCurrent();
    }

    private static EmbeddedPostgres startPostgres() {
        try {
            return EmbeddedPostgres.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start embedded PostgreSQL for test", ex);
        }
    }

    public static class StubFileStorageService implements FileStorageService {

        private final AtomicInteger uploadSequence = new AtomicInteger(0);
        private final AtomicReference<String> lastFileId = new AtomicReference<>();
        private final AtomicReference<Instant> lastExpiresAt = new AtomicReference<>();
        private final Map<String, byte[]> storedContent = new ConcurrentHashMap<>();
        private volatile boolean failUploads;
        private volatile CountDownLatch uploadEnteredLatch;
        private volatile CountDownLatch releaseUploadLatch;

        private String storeFile(String fileName, String contentType, byte[] content, Instant expiresAt) {
            if (failUploads) {
                throw new IllegalStateException("upload failed");
            }
            var enteredLatch = uploadEnteredLatch;
            if (enteredLatch != null) {
                enteredLatch.countDown();
            }
            var releaseLatch = releaseUploadLatch;
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
            var fileId = "stored-file-" + uploadSequence.incrementAndGet();
            storedContent.put(fileId, content);
            lastFileId.set(fileId);
            lastExpiresAt.set(expiresAt);
            return fileId;
        }

        public String storeFile(String fileName, String contentType, Path contentPath, Instant expiresAt) {
            try {
                return storeFile(fileName, contentType, Files.readAllBytes(contentPath), expiresAt);
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to read staged file from stub storage", ex);
            }
        }

        @Override
        public String generateDownloadUrl(String fileId, Instant expiresAt) {
            lastFileId.set(fileId);
            lastExpiresAt.set(expiresAt);
            return "https://download.example/" + fileId;
        }

        public void reset() {
            uploadSequence.set(0);
            lastFileId.set(null);
            lastExpiresAt.set(null);
            storedContent.clear();
            failUploads = false;
            uploadEnteredLatch = null;
            releaseUploadLatch = null;
        }

        public String getLastFileId() {
            return lastFileId.get();
        }

        public Instant getLastExpiresAt() {
            return lastExpiresAt.get();
        }

        public byte[] getStoredContent(String fileId) {
            return storedContent.get(fileId);
        }

        public void setFailUploads(boolean failUploads) {
            this.failUploads = failUploads;
        }

        public void blockUploads(CountDownLatch uploadEnteredLatch, CountDownLatch releaseUploadLatch) {
            this.uploadEnteredLatch = uploadEnteredLatch;
            this.releaseUploadLatch = releaseUploadLatch;
        }

    }
}
