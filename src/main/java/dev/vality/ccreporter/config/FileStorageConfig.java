package dev.vality.ccreporter.config;

import dev.vality.ccreporter.config.properties.FileStorageProperties;
import dev.vality.file.storage.FileStorageSrv;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.http.HttpClient;
import java.lang.reflect.Proxy;

@Configuration
public class FileStorageConfig {

    @Bean
    public HttpClient httpClient() {
        return HttpClient.newHttpClient();
    }

    @Bean
    public FileStorageSrv.Iface fileStorageClient(FileStorageProperties fileStorageProperties) {
        return new THSpawnClientBuilder()
                .withAddress(URI.create(fileStorageProperties.getUrl()))
                .withNetworkTimeout(fileStorageProperties.getNetworkTimeout())
                .build(FileStorageSrv.Iface.class);
    }
}
