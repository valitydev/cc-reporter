package dev.vality.ccreporter.service;

import java.time.Instant;

public interface FileStorageClient {

    String storeFile(String fileName, String contentType, byte[] content, Instant expiresAt)
            throws FileStorageClientException;

    String generateDownloadUrl(String fileId, Instant expiresAt)
            throws FileStorageClientException, StoredFileNotFoundException;
}
