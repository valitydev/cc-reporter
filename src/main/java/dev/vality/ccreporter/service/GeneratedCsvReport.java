package dev.vality.ccreporter.service;

import java.time.Instant;

public record GeneratedCsvReport(
        String fileName,
        String contentType,
        byte[] content,
        long rowsCount,
        Instant dataSnapshotFixedAt
) {
}
