package dev.vality.ccreporter.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ccr.storage.file-storage")
public class FileStorageProperties {

    private String url;
    private int networkTimeout;

}
