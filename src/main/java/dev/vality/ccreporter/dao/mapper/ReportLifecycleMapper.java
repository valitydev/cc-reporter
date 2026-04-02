package dev.vality.ccreporter.dao.mapper;

import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.domain.tables.records.ReportFileRecord;
import lombok.experimental.UtilityClass;
import org.jooq.DSLContext;

import java.time.Instant;
import java.time.ZoneOffset;

import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;

@UtilityClass
public class ReportLifecycleMapper {

    private static final dev.vality.ccreporter.domain.enums.FileType CSV_FILE_TYPE =
            dev.vality.ccreporter.domain.enums.FileType.csv;

    public static ReportFileRecord newInsertableFileRecord(
            DSLContext dslContext,
            long reportId,
            ReportFile reportFile,
            Instant createdAt
    ) {
        var insertableReportFile = new ReportFile(reportFile)
                .setReportId(reportId)
                .setFileType(CSV_FILE_TYPE)
                .setCreatedAt(createdAt.atZone(ZoneOffset.UTC).toLocalDateTime());
        var record = dslContext.newRecord(REPORT_FILE, insertableReportFile);
        record.changed(REPORT_FILE.ID, false);
        return record;
    }
}
