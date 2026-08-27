package dev.vality.ccreporter.report;

import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import dev.vality.ccreporter.constants.ReportAuditEventType;
import dev.vality.ccreporter.dao.ReportAuditDao;
import dev.vality.ccreporter.model.RequestAuditMetadata;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class ReportAuditService {

    private final ReportAuditDao reportAuditDao;

    public void writeReportCreated(
            long reportId,
            String createdBy,
            RequestAuditMetadata auditMetadata,
            CreateReportRequest request,
            String timezone
    ) {
        reportAuditDao.insertEvent(
                reportId,
                ReportAuditEventType.REPORT_CREATED.getEventType(),
                createdBy,
                auditMetadata,
                new CreateReportDetails(
                        request.getReportType().name(),
                        request.getFileType().name(),
                        request.getIdempotencyKey(),
                        timezone
                )
        );
    }

    public void writeReportCanceled(
            long reportId,
            String createdBy,
            RequestAuditMetadata auditMetadata,
            boolean stateChanged
    ) {
        reportAuditDao.insertEvent(
                reportId,
                ReportAuditEventType.REPORT_CANCELED.getEventType(),
                createdBy,
                auditMetadata,
                new CancelReportDetails(stateChanged)
        );
    }

    public void writePresignedUrlGenerated(
            long reportId,
            String createdBy,
            RequestAuditMetadata auditMetadata,
            GeneratePresignedUrlRequest request,
            Instant effectiveExpiresAt,
            String fileId
    ) {
        reportAuditDao.insertEvent(
                reportId,
                ReportAuditEventType.PRESIGNED_URL_GENERATED.getEventType(),
                createdBy,
                auditMetadata,
                new PresignedUrlGeneratedDetails(
                        fileId,
                        request.isSetRequestedExpiresAt() ? request.getRequestedExpiresAt() : null,
                        effectiveExpiresAt.toString()
                )
        );
    }

    private record CreateReportDetails(
            String reportType,
            String fileType,
            String idempotencyKey,
            String timezone
    ) {
    }

    private record CancelReportDetails(
            boolean stateChanged
    ) {
    }

    private record PresignedUrlGeneratedDetails(
            String fileId,
            String requestedExpiresAt,
            String effectiveExpiresAt
    ) {
    }
}
