package dev.vality.ccreporter.handler;

import dev.vality.ccreporter.BadContinuationToken;
import dev.vality.ccreporter.CancelReportRequest;
import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.FileNotFound;
import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.GetReportsRequest;
import dev.vality.ccreporter.GetReportsResponse;
import dev.vality.ccreporter.InvalidRequest;
import dev.vality.ccreporter.Report;
import dev.vality.ccreporter.ReportNotFound;
import dev.vality.ccreporter.ReportingSrv;
import dev.vality.ccreporter.service.ReportManagementService;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Component
public class ReportingHandler implements ReportingSrv.Iface {

    private final ReportManagementService reportManagementService;

    public ReportingHandler(ReportManagementService reportManagementService) {
        this.reportManagementService = reportManagementService;
    }

    @Override
    public long createReport(CreateReportRequest request) throws InvalidRequest, TException {
        return reportManagementService.createReport(request);
    }

    @Override
    public Report getReport(GetReportRequest request) throws ReportNotFound, TException {
        return reportManagementService.getReport(request);
    }

    @Override
    public GetReportsResponse getReports(GetReportsRequest request)
            throws InvalidRequest, BadContinuationToken, TException {
        return reportManagementService.getReports(request);
    }

    @Override
    public void cancelReport(CancelReportRequest request) throws ReportNotFound, TException {
        reportManagementService.cancelReport(request);
    }

    @Override
    public String generatePresignedUrl(GeneratePresignedUrlRequest request)
            throws FileNotFound, InvalidRequest, TException {
        return reportManagementService.generatePresignedUrl(request);
    }
}
