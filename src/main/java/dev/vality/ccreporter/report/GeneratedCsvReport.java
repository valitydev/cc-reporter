package dev.vality.ccreporter.report;

import java.nio.file.Path;
import java.time.Instant;

public record GeneratedCsvReport(
        String fileName,
        String contentType,
        Path contentPath,
        long sizeBytes,
        String md5,
        String sha256,
        long rowsCount,
        Instant dataSnapshotFixedAt
) {
}
