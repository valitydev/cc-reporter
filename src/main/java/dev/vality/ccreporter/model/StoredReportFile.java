package dev.vality.ccreporter.model;

import dev.vality.ccreporter.FileType;

import java.time.Instant;

public record StoredReportFile(
        String fileId,
        FileType fileType,
        String filename,
        String contentType,
        String md5,
        String sha256,
        Long sizeBytes,
        Instant createdAt
) {
}
