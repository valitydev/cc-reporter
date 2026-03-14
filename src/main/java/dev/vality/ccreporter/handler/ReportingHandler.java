package dev.vality.ccreporter.handler;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.report.ReportManagementService;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ReportingHandler implements ReportingSrv.Iface {

    private final ReportManagementService reportManagementService;

    @Override
    public long createReport(CreateReportRequest request) throws TException {
        return reportManagementService.createReport(request);
    }

    @Override
    public Report getReport(GetReportRequest request) throws TException {
        return reportManagementService.getReport(request);
    }

    @Override
    public GetReportsResponse getReports(GetReportsRequest request)
            throws TException {
        return reportManagementService.getReports(request);
    }

    @Override
    public void cancelReport(CancelReportRequest request) throws TException {
        reportManagementService.cancelReport(request);
    }

    @Override
    public String generatePresignedUrl(GeneratePresignedUrlRequest request)
            throws TException {
        return reportManagementService.generatePresignedUrl(request);
    }
}
