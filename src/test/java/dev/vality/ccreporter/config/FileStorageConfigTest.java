package dev.vality.ccreporter.config;

import dev.vality.ccreporter.config.properties.FileStorageProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class FileStorageConfigTest {

    @Test
    void httpClientUsesConfiguredNetworkTimeout() {
        var properties = new FileStorageProperties();
        properties.setNetworkTimeout(1_234);

        var httpClient = new FileStorageConfig().httpClient(properties);

        assertThat(httpClient.connectTimeout()).contains(Duration.ofMillis(1_234));
    }
}
