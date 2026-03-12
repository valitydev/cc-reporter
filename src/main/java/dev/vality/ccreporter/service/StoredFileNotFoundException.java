package dev.vality.ccreporter.service;

public class StoredFileNotFoundException extends RuntimeException {

    public StoredFileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public StoredFileNotFoundException(String message) {
        super(message);
    }
}
