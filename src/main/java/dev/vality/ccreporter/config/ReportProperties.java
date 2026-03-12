package dev.vality.ccreporter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ccr.report")
public class ReportProperties {

    private int presignedUrlTtlSec = 900;

    public int getPresignedUrlTtlSec() {
        return presignedUrlTtlSec;
    }

    public void setPresignedUrlTtlSec(int presignedUrlTtlSec) {
        this.presignedUrlTtlSec = presignedUrlTtlSec;
    }
}
