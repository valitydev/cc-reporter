package dev.vality.ccreporter.service;

public record ReportFileMetadata(
        String fileId,
        String fileName,
        String contentType,
        long sizeBytes,
        String md5,
        String sha256,
        String bucket,
        String objectKey
) {
}
