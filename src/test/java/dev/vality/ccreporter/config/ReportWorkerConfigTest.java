package dev.vality.ccreporter.config;

import com.zaxxer.hikari.HikariDataSource;
import dev.vality.ccreporter.config.properties.ReportProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReportWorkerConfigTest {

    @Test
    void rejectsPoolWithoutConnectionReservedForLifecycleTransitions() {
        var reportProperties = new ReportProperties();
        reportProperties.setWorkerConcurrency(2);
        try (var dataSource = new HikariDataSource()) {
            dataSource.setMaximumPoolSize(3);

            assertThatThrownBy(() -> new ReportWorkerConfig().reportWorkerExecutor(reportProperties, dataSource))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("maximum-pool-size must be at least 4");
        }
    }

    @Test
    void acceptsPoolWithMinimumConnectionReserve() {
        var reportProperties = new ReportProperties();
        reportProperties.setWorkerConcurrency(2);
        try (var dataSource = new HikariDataSource()) {
            dataSource.setMaximumPoolSize(4);

            var executor = new ReportWorkerConfig().reportWorkerExecutor(reportProperties, dataSource);
            try {
                assertThat(executor.isShutdown()).isFalse();
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
