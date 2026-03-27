package dev.vality.ccreporter.handler;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.handler.support.ReportingHandlerLogSupport;
import dev.vality.ccreporter.handler.support.ThriftLoggingHandler;
import dev.vality.ccreporter.report.ReportManagementService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ReportingHandler implements ReportingSrv.Iface, ThriftLoggingHandler {

    private final ReportManagementService reportManagementService;

    @Override
    @SneakyThrows
    public long createReport(CreateReportRequest request) {
        return handleRequest(
                "CreateReport",
                () -> ReportingHandlerLogSupport.summarizeCreateReport(request),
                () -> reportManagementService.createReport(request),
                reportId -> "reportId=" + reportId
        );
    }

    @Override
    @SneakyThrows
    public Report getReport(GetReportRequest request) {
        return handleRequest(
                "GetReport",
                () -> ReportingHandlerLogSupport.summarizeGetReport(request),
                () -> reportManagementService.getReport(request),
                ReportingHandlerLogSupport::summarizeReport
        );
    }

    @Override
    @SneakyThrows
    public GetReportsResponse getReports(GetReportsRequest request) {
        return handleRequest(
                "GetReports",
                () -> ReportingHandlerLogSupport.summarizeGetReports(request),
                () -> reportManagementService.getReports(request),
                ReportingHandlerLogSupport::summarizeGetReportsResponse
        );
    }

    @Override
    @SneakyThrows
    public void cancelReport(CancelReportRequest request) {
        handleRequest(
                "CancelReport",
                () -> ReportingHandlerLogSupport.summarizeCancelReport(request),
                () -> reportManagementService.cancelReport(request)
        );
    }

    @Override
    @SneakyThrows
    public String generatePresignedUrl(GeneratePresignedUrlRequest request) {
        return handleRequest(
                "GeneratePresignedUrl",
                () -> ReportingHandlerLogSupport.summarizeGeneratePresignedUrl(request),
                () -> reportManagementService.generatePresignedUrl(request),
                url -> "urlGenerated=" + (url != null)
        );
    }
}
