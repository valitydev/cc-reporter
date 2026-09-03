package dev.vality.ccreporter.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "api")
public class CcrApiProperties {

    private String path;
    private int defaultPageSize;
    private int maxPageSize;

}
