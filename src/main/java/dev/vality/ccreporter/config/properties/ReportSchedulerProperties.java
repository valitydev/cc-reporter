package dev.vality.ccreporter.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ccr.scheduler")
public class ReportSchedulerProperties {

    private long staleProcessingTimeoutMs;

}
