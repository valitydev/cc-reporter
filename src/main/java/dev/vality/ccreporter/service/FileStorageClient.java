package dev.vality.ccreporter.service;

import java.time.Instant;

public interface FileStorageClient {

    String generateDownloadUrl(String fileId, Instant expiresAt)
            throws FileStorageClientException, StoredFileNotFoundException;
}
