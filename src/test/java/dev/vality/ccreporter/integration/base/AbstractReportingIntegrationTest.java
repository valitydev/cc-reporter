package dev.vality.ccreporter.integration.base;

import dev.vality.ccreporter.ReportingSrv;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.integration.config.ReportingIntegrationTestConfig;
import dev.vality.ccreporter.report.ReportLifecycleService;
import dev.vality.ccreporter.storage.FileStorageService;
import dev.vality.file.storage.FileStorageSrv;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

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
                "ccr.storage.file-storage.url=http://localhost:8022/file-storage",
                "ccr.report.max-attempts=2",
                "ccr.report.expiration-sec=600",
                "ccr.scheduler.stale-processing-timeout-ms=60000"
        }
)
@Import(ReportingIntegrationTestConfig.class)
public abstract class AbstractReportingIntegrationTest {

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
        jdbcTemplate.update("DELETE FROM ccr.withdrawal_session_binding_current");
        jdbcTemplate.update("DELETE FROM ccr.shop_lookup");
        jdbcTemplate.update("DELETE FROM ccr.provider_lookup");
        jdbcTemplate.update("DELETE FROM ccr.terminal_lookup");
        jdbcTemplate.update("DELETE FROM ccr.wallet_lookup");
        stubFileStorageClient.reset();
        bindCaller("user-1");
    }

    @AfterEach
    void tearDownRequestContext() {
        RequestContextHolder.resetRequestAttributes();
    }

    protected void bindCaller(String callerId) {
        var request = new MockHttpServletRequest();
        request.addHeader("X-User-Id", callerId);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    protected void bindCallerWithAuditMetadata(String callerId) {
        var request = new MockHttpServletRequest();
        request.addHeader("X-User-Id", callerId);
        request.addHeader("woody.meta.user-identity.id", "user-id-42");
        request.addHeader("woody.meta.user-identity.username", "alice");
        request.addHeader("woody.meta.user-identity.email", "alice@example.com");
        request.addHeader("woody.meta.user-identity.realm", "external");
        request.addHeader("woody.meta.user-identity.X-Request-ID", "req-123");
        request.addHeader("woody.meta.user-identity.X-Request-Deadline", "2026-01-05T10:15:30Z");
        request.addHeader("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00aa0ba902b7-01");
        request.addHeader("tracestate", "rojo=00f067aa0ba902b7");
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    private static EmbeddedPostgres startPostgres() {
        try {
            return EmbeddedPostgres.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start embedded PostgreSQL for test", ex);
        }
    }

    public static class StubFileStorageService extends FileStorageService {

        private final AtomicInteger uploadSequence = new AtomicInteger(0);
        private final AtomicReference<String> lastFileId = new AtomicReference<>();
        private final AtomicReference<Instant> lastExpiresAt = new AtomicReference<>();
        private final Map<String, byte[]> storedContent = new ConcurrentHashMap<>();
        private volatile boolean failUploads;
        private volatile CountDownLatch uploadEnteredLatch;
        private volatile CountDownLatch releaseUploadLatch;

        public StubFileStorageService() {
            super(missingClient(), java.net.http.HttpClient.newHttpClient());
        }

        @Override
        public String storeFile(String fileName, String contentType, byte[] content, Instant expiresAt) {
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

        @Override
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

        private static FileStorageSrv.Iface missingClient() {
            return (FileStorageSrv.Iface) java.lang.reflect.Proxy.newProxyInstance(
                    FileStorageSrv.Iface.class.getClassLoader(),
                    new Class<?>[]{FileStorageSrv.Iface.class},
                    (proxy, method, args) -> {
                        throw new IllegalStateException("stub file-storage client should not be called directly");
                    }
            );
        }
    }
}
