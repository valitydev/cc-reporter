package dev.vality.ccreporter.storage;

import dev.vality.ccreporter.config.properties.FileStorageProperties;
import dev.vality.file.storage.FileNotFound;
import dev.vality.file.storage.FileStorageSrv;
import dev.vality.file.storage.NewFileResult;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.Collections;

@Component
public class FileStorageService {

    private final FileStorageSrv.Iface fileStorageClient;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public FileStorageService(FileStorageProperties fileStorageProperties) {
        this.fileStorageClient = buildClient(fileStorageProperties);
    }

    public String storeFile(String fileName, String contentType, byte[] content, Instant expiresAt) {
        return uploadFile(contentType, HttpRequest.BodyPublishers.ofByteArray(content), expiresAt);
    }

    public String storeFile(String fileName, String contentType, Path contentPath, Instant expiresAt) {
        try {
            return uploadFile(contentType, HttpRequest.BodyPublishers.ofFile(contentPath), expiresAt);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to stream staged file to file-storage", ex);
        }
    }

    private String uploadFile(String contentType, HttpRequest.BodyPublisher bodyPublisher, Instant expiresAt) {
        if (fileStorageClient == null) {
            throw new IllegalStateException("ccr.storage.file-storage.url is not configured");
        }
        try {
            var newFileResult = fileStorageClient.createNewFile(
                    Collections.emptyMap(),
                    expiresAt.toString()
            );
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(newFileResult.getUploadUrl()))
                    .header("Content-Type", contentType)
                    .PUT(bodyPublisher)
                    .build();
            var response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.discarding()
            );
            if (response.statusCode() / 100 != 2) {
                throw new IllegalStateException(
                        "File upload failed with status: " + response.statusCode()
                );
            }
            return newFileResult.getFileDataId();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to upload file to file-storage", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("File upload interrupted", ex);
        } catch (TException ex) {
            throw new IllegalStateException("Failed to initialize file upload", ex);
        }
    }

    public String generateDownloadUrl(String fileId, Instant expiresAt) throws NoSuchFileException {
        if (fileStorageClient == null) {
            throw new IllegalStateException("ccr.storage.file-storage.url is not configured");
        }
        try {
            return fileStorageClient.generateDownloadUrl(fileId, expiresAt.toString());
        } catch (FileNotFound ex) {
            throw new NoSuchFileException(fileId);
        } catch (TException ex) {
            throw new IllegalStateException(
                    "Failed to generate presigned URL for file: " + fileId,
                    ex
            );
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
