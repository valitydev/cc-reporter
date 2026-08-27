package dev.vality.ccreporter.storage;

import java.nio.file.Path;
import java.time.Instant;

public interface FileStorageService {

    String storeFile(String fileName, String contentType, Path contentPath, Instant expiresAt);

    String generateDownloadUrl(String fileId, Instant expiresAt);
}
