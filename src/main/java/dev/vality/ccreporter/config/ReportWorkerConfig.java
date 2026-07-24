package dev.vality.ccreporter.config;

import com.zaxxer.hikari.HikariDataSource;
import dev.vality.ccreporter.config.properties.ReportProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ReportWorkerConfig {

    private static final int MINIMUM_CONNECTION_RESERVE = 2;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService reportWorkerExecutor(ReportProperties reportProperties, DataSource dataSource) {
        validatePoolCapacity(reportProperties, dataSource);
        var threadFactory = Thread.ofPlatform()
                .name("ccr-report-worker-", 0)
                .factory();
        return Executors.newFixedThreadPool(reportProperties.getWorkerConcurrency(), threadFactory);
    }

    private void validatePoolCapacity(ReportProperties reportProperties, DataSource dataSource) {
        var minimumPoolSize = reportProperties.getWorkerConcurrency() + MINIMUM_CONNECTION_RESERVE;
        if (dataSource instanceof HikariDataSource hikariDataSource
                && hikariDataSource.getMaximumPoolSize() < minimumPoolSize) {
            throw new IllegalStateException(
                    "spring.datasource.hikari.maximum-pool-size must be at least " + minimumPoolSize +
                            " for ccr.report.worker-concurrency=" + reportProperties.getWorkerConcurrency()
            );
        }
    }
}
