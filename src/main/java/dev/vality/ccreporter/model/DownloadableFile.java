package dev.vality.ccreporter.model;

import dev.vality.ccreporter.domain.tables.pojos.ReportFile;

import java.time.Instant;

public record DownloadableFile(ReportFile file, Instant reportExpiresAt) {
}
