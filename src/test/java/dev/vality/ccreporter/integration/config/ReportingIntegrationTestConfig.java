package dev.vality.ccreporter.integration.config;

import dev.vality.ccreporter.integration.base.AbstractReportingIntegrationTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Поднимает тестовые бины для integration-тестов, чтобы не ходить во внешний file storage.
 */
@TestConfiguration
public class ReportingIntegrationTestConfig {

    @Bean
    @Primary
    AbstractReportingIntegrationTest.StubFileStorageService stubFileStorageClient() {
        return new AbstractReportingIntegrationTest.StubFileStorageService();
    }
}
