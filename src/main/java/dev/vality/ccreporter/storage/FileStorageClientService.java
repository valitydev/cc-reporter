package dev.vality.ccreporter.storage;

import dev.vality.file.storage.FileStorageSrv;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;

@Component
@RequiredArgsConstructor
public class FileStorageClientService implements FileStorageService {

    private final FileStorageSrv.Iface fileStorageClient;
    private final HttpClient httpClient;

    @Override
    @SneakyThrows
    public String storeFile(String fileName, String contentType, Path contentPath, Instant expiresAt) {
        return uploadFile(contentType, HttpRequest.BodyPublishers.ofFile(contentPath), expiresAt);
    }

    @Override
    @SneakyThrows
    public String generateDownloadUrl(String fileId, Instant expiresAt) {
        return fileStorageClient.generateDownloadUrl(fileId, expiresAt.toString());
    }

    @SneakyThrows
    private String uploadFile(String contentType, HttpRequest.BodyPublisher bodyPublisher, Instant expiresAt) {
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
    }
}
