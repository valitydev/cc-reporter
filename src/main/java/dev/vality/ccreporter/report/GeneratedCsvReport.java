package dev.vality.ccreporter.report;

import java.time.Instant;

public record GeneratedCsvReport(
        String fileName,
        String contentType,
        byte[] content,
        long rowsCount,
        Instant dataSnapshotFixedAt
) {
}
