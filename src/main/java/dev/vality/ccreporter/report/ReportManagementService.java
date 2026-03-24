package dev.vality.ccreporter.report;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.config.properties.CcrApiProperties;
import dev.vality.ccreporter.config.properties.ReportProperties;
import dev.vality.ccreporter.dao.ReportCommandDao;
import dev.vality.ccreporter.dao.ReportLifecycleDao;
import dev.vality.ccreporter.dao.ReportQueryDao;
import dev.vality.ccreporter.report.mapper.ReportThriftMapper;
import dev.vality.ccreporter.security.RequestAuditMetadataResolver;
import dev.vality.ccreporter.serde.json.ContinuationTokenJsonSerializer;
import dev.vality.ccreporter.storage.FileStorageService;
import dev.vality.ccreporter.util.TimestampUtils;
import lombok.RequiredArgsConstructor;
import org.apache.thrift.TException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ReportManagementService {

    private final ReportCommandDao reportCommandDao;
    private final ReportQueryDao reportQueryDao;
    private final ReportLifecycleDao reportLifecycleDao;
    private final ReportManagementTransactionService reportManagementTransactionService;
    private final ReportAuditService reportAuditService;
    private final ReportRequestValidator reportRequestValidator;
    private final ReportThriftMapper reportThriftMapper;
    private final ContinuationTokenJsonSerializer continuationTokenJsonSerializer;
    private final RequestAuditMetadataResolver requestAuditMetadataResolver;
    private final CcrApiProperties apiProperties;
    private final ReportProperties reportProperties;
    private final FileStorageService fileStorageService;

    public long createReport(CreateReportRequest request) throws InvalidRequest {
        reportRequestValidator.validateCreate(request);
        var auditMetadata = requestAuditMetadataResolver.resolve();
        var timezone = StringUtils.hasText(request.getTimezone()) ? request.getTimezone() : "UTC";
        var createdBy = auditMetadata.email();
        try {
            return reportManagementTransactionService.createReport(createdBy, auditMetadata, request, timezone);
        } catch (DuplicateKeyException ex) {
            return reportCommandDao.findByIdempotencyKey(createdBy, request.getIdempotencyKey())
                    .orElseThrow(() -> ex);
        }
    }

    public Report getReport(GetReportRequest request) throws InvalidRequest, ReportNotFound {
        if (request == null) {
            throw invalidRequest("request is required");
        }
        var createdBy = requestAuditMetadataResolver.resolve().email();
        return reportQueryDao.getReport(createdBy, request.getReportId())
                .map(reportThriftMapper::mapReport)
                .orElseThrow(ReportNotFound::new);
    }

    public GetReportsResponse getReports(GetReportsRequest request) throws InvalidRequest, BadContinuationToken {
        var createdBy = requestAuditMetadataResolver.resolve().email();
        var safeRequest = request == null ? new GetReportsRequest() : request;
        reportRequestValidator.validateGetReports(safeRequest);

        var meta = safeRequest.getMeta();
        var limit = resolveLimit(meta);
        var cursor = meta != null && meta.isSetContinuationToken()
                ? continuationTokenJsonSerializer.deserialize(meta.getContinuationToken())
                : null;
        var storedReports = reportQueryDao.getReports(createdBy, safeRequest.getFilter(), cursor, limit);

        var response = new GetReportsResponse();
        response.setReports(storedReports.stream().map(reportThriftMapper::mapReport).toList());
        if (storedReports.size() == limit) {
            var lastReport = storedReports.getLast();
            response.setContinuationToken(
                    continuationTokenJsonSerializer.serialize(
                            TimestampUtils.toInstant(lastReport.job().getCreatedAt()),
                            lastReport.job().getId()
                    )
            );
        }
        return response;
    }

    @Transactional
    public void cancelReport(CancelReportRequest request) throws InvalidRequest, ReportNotFound {
        if (request == null) {
            throw invalidRequest("request is required");
        }
        var auditMetadata = requestAuditMetadataResolver.resolve();
        var createdBy = auditMetadata.email();
        var updated = reportLifecycleDao.cancelReport(createdBy, request.getReportId(), Instant.now());
        if (!updated && !reportCommandDao.reportExists(createdBy, request.getReportId())) {
            throw new ReportNotFound();
        }
        reportAuditService.writeReportCanceled(request.getReportId(), createdBy, auditMetadata, updated);
    }

    public String generatePresignedUrl(GeneratePresignedUrlRequest request)
            throws TException {
        if (request == null) {
            throw invalidRequest("request is required");
        }
        var auditMetadata = requestAuditMetadataResolver.resolve();
        var createdBy = auditMetadata.email();
        var fileData = reportQueryDao.getFile(createdBy, request.getFileId());
        if (fileData.isEmpty()) {
            throw new FileNotFound();
        }

        var effectiveExpiresAt = resolveEffectivePresignedUrlExpiresAt(request);
        try {
            var url = fileStorageService.generateDownloadUrl(
                    fileData.get().getFileId(),
                    effectiveExpiresAt
            );
            reportAuditService.writePresignedUrlGenerated(
                    fileData.get().getReportId(),
                    createdBy,
                    auditMetadata,
                    request,
                    effectiveExpiresAt,
                    fileData.get().getFileId()
            );
            return url;
        } catch (NoSuchFileException ex) {
            throw new FileNotFound();
        }
    }

    private Instant resolveEffectivePresignedUrlExpiresAt(GeneratePresignedUrlRequest request) throws InvalidRequest {
        var now = Instant.now();
        var ttlCap = now.plusSeconds(reportProperties.getPresignedUrlTtlSec());
        var requestedExpiresAt = request.isSetRequestedExpiresAt()
                ? TimestampUtils.parse(request.getRequestedExpiresAt())
                : ttlCap;
        if (!requestedExpiresAt.isAfter(now)) {
            throw invalidRequest("requested_expires_at must be in the future");
        }
        return requestedExpiresAt.isAfter(ttlCap) ? ttlCap : requestedExpiresAt;
    }

    private int resolveLimit(GetReportsMeta meta) {
        if (meta == null || !meta.isSetLimit()) {
            return apiProperties.getDefaultPageSize();
        }
        return Math.min(meta.getLimit(), apiProperties.getMaxPageSize());
    }

    private InvalidRequest invalidRequest(String error) {
        return new InvalidRequest(List.of(error));
    }
}
