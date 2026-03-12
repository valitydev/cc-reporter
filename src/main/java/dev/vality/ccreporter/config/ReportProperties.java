package dev.vality.ccreporter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ccr.report")
public class ReportProperties {

    private int maxAttempts = 5;
    private int presignedUrlTtlSec = 900;
    private long expirationSec = 604800;

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public int getPresignedUrlTtlSec() {
        return presignedUrlTtlSec;
    }

    public void setPresignedUrlTtlSec(int presignedUrlTtlSec) {
        this.presignedUrlTtlSec = presignedUrlTtlSec;
    }

    public long getExpirationSec() {
        return expirationSec;
    }

    public void setExpirationSec(long expirationSec) {
        this.expirationSec = expirationSec;
    }
}
