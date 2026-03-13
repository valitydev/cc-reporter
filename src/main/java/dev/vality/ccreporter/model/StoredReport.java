package dev.vality.ccreporter.model;

import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;

import java.time.Instant;

public record StoredReport(
        long id,
        ReportType reportType,
        FileType fileType,
        String queryJson,
        String timezone,
        ReportStatus status,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant dataSnapshotFixedAt,
        Instant finishedAt,
        Long rowsCount,
        Instant expiresAt,
        String errorCode,
        String errorMessage,
        ReportFile fileData
) {
}
