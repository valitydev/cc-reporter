package dev.vality.ccreporter.service;

import dev.vality.ccreporter.config.FileStorageProperties;
import dev.vality.file.storage.FileNotFound;
import dev.vality.file.storage.FileStorageSrv;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import java.net.URI;
import java.time.Instant;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class WoodyFileStorageClient implements FileStorageClient {

    private final FileStorageProperties fileStorageProperties;
    private final FileStorageSrv.Iface fileStorageClient;

    public WoodyFileStorageClient(FileStorageProperties fileStorageProperties) {
        this.fileStorageProperties = fileStorageProperties;
        this.fileStorageClient = buildClient(fileStorageProperties);
    }

    @Override
    public String generateDownloadUrl(String fileId, Instant expiresAt) {
        if (fileStorageClient == null) {
            throw new FileStorageClientException("ccr.storage.file-storage.url is not configured");
        }
        try {
            return fileStorageClient.generateDownloadUrl(fileId, expiresAt.toString());
        } catch (FileNotFound ex) {
            throw new StoredFileNotFoundException("File not found in file-storage: " + fileId, ex);
        } catch (TException ex) {
            throw new FileStorageClientException("Failed to generate presigned URL for file: " + fileId, ex);
        }
    }

    private static FileStorageSrv.Iface buildClient(FileStorageProperties properties) {
        if (!StringUtils.hasText(properties.getUrl())) {
            return null;
        }
        return new THSpawnClientBuilder()
                .withAddress(URI.create(properties.getUrl()))
                .withNetworkTimeout(properties.getNetworkTimeout())
                .build(FileStorageSrv.Iface.class);
    }
}
