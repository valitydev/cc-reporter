package dev.vality.ccreporter.service;

import dev.vality.ccreporter.config.FileStorageProperties;
import dev.vality.file.storage.FileNotFound;
import dev.vality.file.storage.FileStorageSrv;
import dev.vality.file.storage.NewFileResult;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Collections;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class WoodyFileStorageClient implements FileStorageClient {

    private final FileStorageProperties fileStorageProperties;
    private final FileStorageSrv.Iface fileStorageClient;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public WoodyFileStorageClient(FileStorageProperties fileStorageProperties) {
        this.fileStorageProperties = fileStorageProperties;
        this.fileStorageClient = buildClient(fileStorageProperties);
    }

    @Override
    public String storeFile(String fileName, String contentType, byte[] content, Instant expiresAt) {
        if (fileStorageClient == null) {
            throw new FileStorageClientException("ccr.storage.file-storage.url is not configured");
        }
        try {
            NewFileResult newFileResult = fileStorageClient.createNewFile(Collections.emptyMap(), expiresAt.toString());
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(newFileResult.getUploadUrl()))
                    .header("Content-Type", contentType)
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(content))
                    .build();
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() / 100 != 2) {
                throw new FileStorageClientException("File upload failed with status: " + response.statusCode());
            }
            return newFileResult.getFileDataId();
        } catch (IOException ex) {
            throw new FileStorageClientException("Failed to upload file to file-storage", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileStorageClientException("File upload interrupted", ex);
        } catch (TException ex) {
            throw new FileStorageClientException("Failed to initialize file upload", ex);
        }
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
