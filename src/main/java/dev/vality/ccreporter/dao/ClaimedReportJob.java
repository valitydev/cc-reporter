package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportType;

public record ClaimedReportJob(
        long id,
        ReportType reportType,
        FileType fileType,
        String queryJson,
        String timezone,
        int attempt
) {
}
