package dev.vality.ccreporter.report;

import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.dao.ReportCommandDao;
import dev.vality.ccreporter.model.RequestAuditMetadata;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ReportManagementTransactionService {

    private final ReportCommandDao reportCommandDao;
    private final ReportAuditService reportAuditService;

    @Transactional
    public long createReport(
            String createdBy,
            RequestAuditMetadata auditMetadata,
            CreateReportRequest request,
            String timezone
    ) {
        var reportId = reportCommandDao.createReport(
                createdBy,
                request.getReportType(),
                request.getFileType(),
                request.getQuery(),
                timezone,
                request.getIdempotencyKey()
        );
        reportAuditService.writeReportCreated(reportId, createdBy, auditMetadata, request, timezone);
        return reportId;
    }
}
