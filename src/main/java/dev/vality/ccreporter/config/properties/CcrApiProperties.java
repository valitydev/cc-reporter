package dev.vality.ccreporter.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ccr.api")
public class CcrApiProperties {

    private String path;
    private String createdByHeader;
    private int defaultPageSize;
    private int maxPageSize;

}
