package dev.vality.ccreporter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ccr.api")
public class CcrApiProperties {

    private String path = "/ccreports";
    private String createdByHeader = "X-User-Id";
    private int defaultPageSize = 50;
    private int maxPageSize = 100;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getCreatedByHeader() {
        return createdByHeader;
    }

    public void setCreatedByHeader(String createdByHeader) {
        this.createdByHeader = createdByHeader;
    }

    public int getDefaultPageSize() {
        return defaultPageSize;
    }

    public void setDefaultPageSize(int defaultPageSize) {
        this.defaultPageSize = defaultPageSize;
    }

    public int getMaxPageSize() {
        return maxPageSize;
    }

    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }
}
