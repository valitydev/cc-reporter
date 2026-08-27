package dev.vality.ccreporter.report.mapper;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.dao.mapper.ReportRecordMapper;
import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.model.ReportProjection;
import dev.vality.ccreporter.serde.json.ThriftJsonCodec;
import dev.vality.ccreporter.util.TimestampUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
@RequiredArgsConstructor
public class ReportThriftMapper {

    private final ThriftJsonCodec thriftJsonCodec;

    public Report mapReport(ReportProjection reportProjection) {
        var reportJob = reportProjection.job();
        var report = new Report();
        report.setReportId(reportJob.getId());
        report.setReportType(ReportRecordMapper.mapEnum(reportJob.getReportType(), ReportType.class));
        report.setFileType(ReportRecordMapper.mapEnum(reportJob.getFileType(), FileType.class));
        report.setQuery(thriftJsonCodec.deserialize(reportJob.getQueryJson().data(), ReportQuery.class));
        report.setCreatedAt(TimestampUtils.format(TimestampUtils.toInstant(reportJob.getCreatedAt())));
        report.setStatus(ReportRecordMapper.mapEnum(reportJob.getStatus(), ReportStatus.class));
        setTimestampIfPresent(report::setStartedAt, reportJob.getStartedAt());
        setTimestampIfPresent(report::setDataSnapshotFixedAt, reportJob.getDataSnapshotFixedAt());
        setTimestampIfPresent(report::setFinishedAt, reportJob.getFinishedAt());
        if (reportJob.getRowsCount() != null) {
            report.setRowsCount(reportJob.getRowsCount());
        }
        setTimestampIfPresent(report::setExpiresAt, reportJob.getExpiresAt());
        if (StringUtils.hasText(reportJob.getErrorCode()) || StringUtils.hasText(reportJob.getErrorMessage())) {
            report.setError(new ErrorInfo(
                    defaultString(reportJob.getErrorCode()),
                    defaultString(reportJob.getErrorMessage())
            ));
        }
        if (reportProjection.file() != null) {
            report.setFile(mapFile(reportProjection.file()));
        }
        return report;
    }

    public FileMeta mapFile(ReportFile fileData) {
        var fileMeta = new FileMeta();
        fileMeta.setFileId(fileData.getFileId());
        fileMeta.setFileType(ReportRecordMapper.mapEnum(fileData.getFileType(), FileType.class));
        fileMeta.setFilename(fileData.getFilename());
        fileMeta.setContentType(fileData.getContentType());
        fileMeta.setSignature(new FileSignature(fileData.getMd5(), fileData.getSha256()));
        if (fileData.getSizeBytes() != null) {
            fileMeta.setSizeBytes(fileData.getSizeBytes());
        }
        fileMeta.setCreatedAt(TimestampUtils.format(TimestampUtils.toInstant(fileData.getCreatedAt())));
        return fileMeta;
    }

    private void setTimestampIfPresent(java.util.function.Consumer<String> setter, java.time.LocalDateTime value) {
        if (value != null) {
            setter.accept(TimestampUtils.format(TimestampUtils.toInstant(value)));
        }
    }

    private String defaultString(String value) {
        return value == null ? "" : value;
    }
}
