package dev.vality.ccreporter.model;

import dev.vality.ccreporter.domain.enums.ReportType;

public record ReportTask(
        long id,
        ReportType reportType,
        String queryJson,
        String timezone,
        int attempt
) {
}
