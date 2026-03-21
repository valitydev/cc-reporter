package dev.vality.ccreporter.dao;

import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.model.StoredReport;
import dev.vality.ccreporter.model.StoredReportFile;
import lombok.experimental.UtilityClass;
import org.jooq.Record;
import org.jooq.TableField;

import java.sql.Timestamp;

import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;
import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;
import static dev.vality.ccreporter.util.TimestampUtils.*;

@UtilityClass
public class ReportRecordMapper {

    public static StoredReport mapReport(Record record) {
        var reportFileType = REPORT_FILE.FILE_TYPE.as("report_file_type");
        var createdAt = timestampField(REPORT_JOB.CREATED_AT, "created_at_ts");
        var updatedAt = timestampField(REPORT_JOB.UPDATED_AT, "updated_at_ts");
        var startedAt = timestampField(REPORT_JOB.STARTED_AT, "started_at_ts");
        var dataSnapshotFixedAt = timestampField(REPORT_JOB.DATA_SNAPSHOT_FIXED_AT, "data_snapshot_fixed_at_ts");
        var finishedAt = timestampField(REPORT_JOB.FINISHED_AT, "finished_at_ts");
        var expiresAt = timestampField(REPORT_JOB.EXPIRES_AT, "expires_at_ts");
        var fileCreatedAt = timestampField(REPORT_FILE.CREATED_AT, "file_created_at_ts");
        return new StoredReport(
                record.get(REPORT_JOB.ID),
                fromJooqReportType(record.get(REPORT_JOB.REPORT_TYPE)),
                fromJooqFileType(record.get(REPORT_JOB.FILE_TYPE)),
                record.get(REPORT_JOB.QUERY_JSON).data(),
                record.get(REPORT_JOB.TIMEZONE),
                fromJooqReportStatus(record.get(REPORT_JOB.STATUS)),
                toInstant(record.get(createdAt)),
                toInstant(record.get(updatedAt)),
                toOptionalInstant(record.get(startedAt)),
                toOptionalInstant(record.get(dataSnapshotFixedAt)),
                toOptionalInstant(record.get(finishedAt)),
                record.get(REPORT_JOB.ROWS_COUNT),
                toOptionalInstant(record.get(expiresAt)),
                record.get(REPORT_JOB.ERROR_CODE),
                record.get(REPORT_JOB.ERROR_MESSAGE),
                record.get(REPORT_FILE.FILE_ID) == null ? null : mapStoredReportFile(
                        record.get(REPORT_FILE.FILE_ID),
                        record.get(reportFileType),
                        record.get(REPORT_FILE.FILENAME),
                        record.get(REPORT_FILE.CONTENT_TYPE),
                        record.get(REPORT_FILE.MD5),
                        record.get(REPORT_FILE.SHA256),
                        record.get(REPORT_FILE.SIZE_BYTES),
                        record.get(fileCreatedAt)
                )
        );
    }

    public static ReportFile mapFile(Record record) {
        return new ReportFile()
                .setReportId(record.get(REPORT_FILE.REPORT_ID))
                .setFileId(record.get(REPORT_FILE.FILE_ID))
                .setFileType(record.get(REPORT_FILE.FILE_TYPE))
                .setFilename(record.get(REPORT_FILE.FILENAME))
                .setContentType(record.get(REPORT_FILE.CONTENT_TYPE))
                .setMd5(record.get(REPORT_FILE.MD5))
                .setSha256(record.get(REPORT_FILE.SHA256))
                .setSizeBytes(record.get(REPORT_FILE.SIZE_BYTES))
                .setCreatedAt(toLocalDateTime(
                        toInstant(record.get(timestampField(REPORT_FILE.CREATED_AT, "created_at_ts")))
                ))
                .setBucket(record.get(REPORT_FILE.BUCKET))
                .setObjectKey(record.get(REPORT_FILE.OBJECT_KEY));
    }

    public static org.jooq.Field<Timestamp> timestampField(TableField<?, ?> field, String alias) {
        return org.jooq.impl.DSL.field(field.getQualifiedName(), Timestamp.class).as(alias);
    }

    public static dev.vality.ccreporter.domain.enums.ReportStatus toJooqReportStatus(ReportStatus status) {
        return dev.vality.ccreporter.domain.enums.ReportStatus.valueOf(status.name());
    }

    public static dev.vality.ccreporter.domain.enums.ReportType toJooqReportType(ReportType reportType) {
        return dev.vality.ccreporter.domain.enums.ReportType.valueOf(reportType.name());
    }

    public static dev.vality.ccreporter.domain.enums.FileType toJooqFileType(FileType fileType) {
        return dev.vality.ccreporter.domain.enums.FileType.valueOf(fileType.name());
    }

    private static ReportStatus fromJooqReportStatus(dev.vality.ccreporter.domain.enums.ReportStatus status) {
        return ReportStatus.valueOf(status.getLiteral());
    }

    private static ReportType fromJooqReportType(dev.vality.ccreporter.domain.enums.ReportType reportType) {
        return ReportType.valueOf(reportType.getLiteral());
    }

    private static FileType fromJooqFileType(dev.vality.ccreporter.domain.enums.FileType fileType) {
        return FileType.valueOf(fileType.getLiteral());
    }

    private static StoredReportFile mapStoredReportFile(
            String fileId,
            dev.vality.ccreporter.domain.enums.FileType fileType,
            String filename,
            String contentType,
            String md5,
            String sha256,
            Long sizeBytes,
            Timestamp createdAt
    ) {
        return new StoredReportFile(
                fileId,
                fromJooqFileType(fileType),
                filename,
                contentType,
                md5,
                sha256,
                sizeBytes,
                toInstant(createdAt)
        );
    }
}
