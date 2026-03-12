package dev.vality.ccreporter.service;

public class FileStorageClientException extends RuntimeException {

    public FileStorageClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileStorageClientException(String message) {
        super(message);
    }
}
