package dev.vality.ccreporter.dao.mapper;

import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.domain.tables.records.ReportFileRecord;
import dev.vality.ccreporter.model.ClaimedReportJob;
import dev.vality.ccreporter.model.ReportFileMetadata;
import lombok.experimental.UtilityClass;
import org.jooq.DSLContext;
import org.jooq.Record6;

import java.time.Instant;
import java.time.ZoneOffset;

import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;

@UtilityClass
public class ReportLifecycleMapper {

    private static final dev.vality.ccreporter.domain.enums.FileType CSV_FILE_TYPE =
            dev.vality.ccreporter.domain.enums.FileType.csv;

    public static ClaimedReportJob mapClaimedReport(Record6<Long,
            dev.vality.ccreporter.domain.enums.ReportType,
            dev.vality.ccreporter.domain.enums.FileType,
            org.jooq.JSONB,
            String,
            Integer> record) {
        return new ClaimedReportJob(
                record.value1(),
                ReportRecordMapper.mapEnum(record.value2(), dev.vality.ccreporter.ReportType.class),
                ReportRecordMapper.mapEnum(record.value3(), dev.vality.ccreporter.FileType.class),
                record.value4().data(),
                record.value5(),
                record.value6()
        );
    }

    public static ReportFileRecord newInsertableFileRecord(
            DSLContext dslContext,
            long reportId,
            ReportFileMetadata fileMetadata,
            Instant createdAt
    ) {
        var reportFile = new ReportFile()
                .setReportId(reportId)
                .setFileId(fileMetadata.fileId())
                .setFileType(CSV_FILE_TYPE)
                .setBucket(fileMetadata.bucket())
                .setObjectKey(fileMetadata.objectKey())
                .setFilename(fileMetadata.fileName())
                .setContentType(fileMetadata.contentType())
                .setSizeBytes(fileMetadata.sizeBytes())
                .setMd5(fileMetadata.md5())
                .setSha256(fileMetadata.sha256())
                .setCreatedAt(createdAt.atZone(ZoneOffset.UTC).toLocalDateTime());
        var record = dslContext.newRecord(REPORT_FILE, reportFile);
        record.changed(REPORT_FILE.ID, false);
        return record;
    }
}
