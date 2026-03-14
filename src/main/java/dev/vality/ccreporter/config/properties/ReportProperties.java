package dev.vality.ccreporter.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ccr.report")
public class ReportProperties {

    private int maxAttempts;
    private int presignedUrlTtlSec;
    private long expirationSec;

}
