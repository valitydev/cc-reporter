package dev.vality.ccreporter.service;

import dev.vality.ccreporter.FileType;
import java.time.Instant;

public record StoredFileData(
        String fileId,
        FileType fileType,
        String filename,
        String contentType,
        String md5,
        String sha256,
        Long sizeBytes,
        Instant createdAt,
        String bucket,
        String objectKey
) {
}
