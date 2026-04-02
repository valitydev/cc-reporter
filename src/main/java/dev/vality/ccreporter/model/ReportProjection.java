package dev.vality.ccreporter.model;

import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.domain.tables.pojos.ReportJob;

public record ReportProjection(
        ReportJob job,
        ReportFile file
) {
}
