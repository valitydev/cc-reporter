package dev.vality.ccreporter.dao.mapper;

import dev.vality.ccreporter.domain.tables.pojos.ReportFile;
import dev.vality.ccreporter.domain.tables.pojos.ReportJob;
import dev.vality.ccreporter.model.ReportProjection;
import lombok.experimental.UtilityClass;
import org.jooq.Record;

import static dev.vality.ccreporter.domain.Tables.REPORT_FILE;
import static dev.vality.ccreporter.domain.Tables.REPORT_JOB;

@UtilityClass
public class ReportRecordMapper {

    public static ReportProjection mapReportProjection(Record record) {
        return new ReportProjection(
                mapReportJob(record),
                record.get(REPORT_FILE.FILE_ID) == null ? null : mapReportFile(record)
        );
    }

    public static ReportJob mapReportJob(org.jooq.Record record) {
        return record.into(REPORT_JOB).into(ReportJob.class);
    }

    public static ReportFile mapReportFile(Record record) {
        return record.into(REPORT_FILE).into(ReportFile.class);
    }

    public static <S extends Enum<S>, T extends Enum<T>> T mapEnum(S source, Class<T> targetClass) {
        if (source == null) {
            return null;
        }
        return Enum.valueOf(targetClass, source.name());
    }
}
