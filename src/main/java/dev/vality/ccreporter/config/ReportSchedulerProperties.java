package dev.vality.ccreporter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ccr.scheduler")
public class ReportSchedulerProperties {

    private boolean enabled;
    private long pollIntervalMs = 10000;
    private long staleProcessingTimeoutMs = 300000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public long getStaleProcessingTimeoutMs() {
        return staleProcessingTimeoutMs;
    }

    public void setStaleProcessingTimeoutMs(long staleProcessingTimeoutMs) {
        this.staleProcessingTimeoutMs = staleProcessingTimeoutMs;
    }
}
