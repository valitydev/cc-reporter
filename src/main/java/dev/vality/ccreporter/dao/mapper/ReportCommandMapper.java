package dev.vality.ccreporter.dao.mapper;

import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.domain.tables.pojos.ReportJob;
import dev.vality.ccreporter.domain.tables.records.ReportJobRecord;
import lombok.experimental.UtilityClass;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.util.StringUtils;

import java.time.Instant;

import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;
import static dev.vality.ccreporter.util.TimestampUtils.toLocalDateTime;

@UtilityClass
public class ReportCommandMapper {

    public static ReportJob mapReportJob(
            String createdBy,
            ReportType reportType,
            FileType fileType,
            String queryJson,
            String queryHash,
            Instant requestedTimeFrom,
            Instant requestedTimeTo,
            String timezone,
            String idempotencyKey
    ) {
        return new ReportJob()
                .setReportType(
                        ReportRecordMapper.mapEnum(reportType, dev.vality.ccreporter.domain.enums.ReportType.class))
                .setFileType(ReportRecordMapper.mapEnum(fileType, dev.vality.ccreporter.domain.enums.FileType.class))
                .setQueryJson(JSONB.jsonb(queryJson))
                .setQueryHash(queryHash)
                .setRequestedTimeFrom(toLocalDateTime(requestedTimeFrom))
                .setRequestedTimeTo(toLocalDateTime(requestedTimeTo))
                .setTimezone(timezone)
                .setCreatedBy(createdBy)
                .setIdempotencyKey(StringUtils.hasText(idempotencyKey) ? idempotencyKey : null);
    }

    public static ReportJobRecord newInsertableRecord(DSLContext dslContext, ReportJob reportJob) {
        var record = dslContext.newRecord(REPORT_JOB, reportJob);
        record.changed(REPORT_JOB.ID, false);
        record.changed(REPORT_JOB.STATUS, false);
        record.changed(REPORT_JOB.ATTEMPT, false);
        record.changed(REPORT_JOB.CREATED_AT, false);
        record.changed(REPORT_JOB.UPDATED_AT, false);
        return record;
    }
}
