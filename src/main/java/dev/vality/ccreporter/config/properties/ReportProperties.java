package dev.vality.ccreporter.config.properties;

import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "report")
public class ReportProperties {

    private int maxAttempts;
    @Positive
    private int workerConcurrency;
    @Positive
    private long processingTimeoutMs;
    private int presignedUrlTtlSec;
    private long expirationSec;

}
