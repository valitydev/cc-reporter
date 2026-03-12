package dev.vality.ccreporter.integration;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
class ReportingIntegrationTestConfig {

    @Bean
    @Primary
    AbstractReportingIntegrationTest.StubFileStorageClient stubFileStorageClient() {
        return new AbstractReportingIntegrationTest.StubFileStorageClient();
    }
}
