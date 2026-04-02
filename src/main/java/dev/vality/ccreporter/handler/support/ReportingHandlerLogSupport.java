package dev.vality.ccreporter.handler.support;

import dev.vality.ccreporter.*;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ReportingHandlerLogSupport {

    public static String summarizeCreateReport(CreateReportRequest request) {
        if (request == null) {
            return "request=null";
        }
        return "reportType=" + request.getReportType() +
                ", fileType=" + request.getFileType() +
                ", timezone=" + request.getTimezone() +
                ", idempotencyKeyPresent=" + request.isSetIdempotencyKey() +
                ", queryBranch=" + request.getQuery().getSetField().getFieldName();
    }

    public static String summarizeGetReport(GetReportRequest request) {
        return request == null ? "request=null" : "reportId=" + request.getReportId();
    }

    public static String summarizeGetReports(GetReportsRequest request) {
        if (request == null) {
            return "request=null";
        }
        var meta = request.getMeta();
        var filter = request.getFilter();
        return "limit=" + (meta != null && meta.isSetLimit() ? meta.getLimit() : null) +
                ", continuationTokenPresent=" + (meta != null && meta.isSetContinuationToken()) +
                ", statusesCount=" + (filter != null && filter.isSetStatuses() ? filter.getStatusesSize() : 0) +
                ", reportTypesCount=" + (filter != null && filter.isSetReportTypes()
                ? filter.getReportTypesSize()
                : 0) +
                ", fileTypesCount=" + (filter != null && filter.isSetFileTypes() ? filter.getFileTypesSize() : 0) +
                ", createdFromPresent=" + (filter != null && filter.isSetCreatedFrom()) +
                ", createdToPresent=" + (filter != null && filter.isSetCreatedTo());
    }

    public static String summarizeCancelReport(CancelReportRequest request) {
        return request == null ? "request=null" : "reportId=" + request.getReportId();
    }

    public static String summarizeGeneratePresignedUrl(GeneratePresignedUrlRequest request) {
        if (request == null) {
            return "request=null";
        }
        return "fileId=" + request.getFileId() +
                ", requestedExpiresAtPresent=" + request.isSetRequestedExpiresAt();
    }

    public static String summarizeReport(Report report) {
        if (report == null) {
            return "report=null";
        }
        return "reportId=" + report.getReportId() +
                ", status=" + report.getStatus() +
                ", filePresent=" + report.isSetFile();
    }

    public static String summarizeGetReportsResponse(GetReportsResponse response) {
        if (response == null) {
            return "response=null";
        }
        return "reportsCount=" + response.getReportsSize() +
                ", continuationTokenPresent=" + response.isSetContinuationToken();
    }
}
