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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ReportManagementService {

    private final ReportCommandDao reportCommandDao;
    private final ReportQueryDao reportQueryDao;
    private final ReportLifecycleDao reportLifecycleDao;
    private final ReportAuditService reportAuditService;
    private final ReportRequestValidator reportRequestValidator;
    private final ReportThriftMapper reportThriftMapper;
    private final ContinuationTokenJsonSerializer continuationTokenJsonSerializer;
    private final RequestAuditMetadataResolver requestAuditMetadataResolver;
    private final CcrApiProperties apiProperties;
    private final ReportProperties reportProperties;
    private final FileStorageService fileStorageService;

    @Transactional
    public long createReport(CreateReportRequest request) throws InvalidRequest {
        reportRequestValidator.validateCreate(request);
        var auditMetadata = requestAuditMetadataResolver.resolve();
        var timezone = StringUtils.hasText(request.getTimezone()) ? request.getTimezone() : "UTC";
        var createdBy = auditMetadata.email();
        var result = reportCommandDao.createReport(
                createdBy,
                request.getReportType(),
                request.getFileType(),
                request.getQuery(),
                timezone,
                request.getIdempotencyKey()
        );
        if (result.created()) {
            reportAuditService.writeReportCreated(result.reportId(), createdBy, auditMetadata, request, timezone);
        }
        return result.reportId();
    }

    @Transactional
    public Report getReport(GetReportRequest request) throws InvalidRequest, ReportNotFound {
        if (request == null) {
            throw invalidRequest("request is required");
        }
        var createdBy = requestAuditMetadataResolver.resolve().email();
        reportLifecycleDao.expireReports(Instant.now());
        return reportQueryDao.getReport(createdBy, request.getReportId())
                .map(reportThriftMapper::mapReport)
                .orElseThrow(ReportNotFound::new);
    }

    @Transactional
    public GetReportsResponse getReports(GetReportsRequest request) throws InvalidRequest, BadContinuationToken {
        var createdBy = requestAuditMetadataResolver.resolve().email();
        var safeRequest = request == null ? new GetReportsRequest() : request;
        reportRequestValidator.validateGetReports(safeRequest);
        reportLifecycleDao.expireReports(Instant.now());

        var meta = safeRequest.getMeta();
        var limit = resolveLimit(meta);
        var cursor = meta != null && meta.isSetContinuationToken()
                ? continuationTokenJsonSerializer.deserialize(meta.getContinuationToken())
                : null;
        var storedReports = reportQueryDao.getReports(createdBy, safeRequest.getFilter(), cursor, limit + 1);
        var hasNextPage = storedReports.size() > limit;
        var page = hasNextPage ? storedReports.subList(0, limit) : storedReports;

        var response = new GetReportsResponse();
        response.setReports(page.stream().map(reportThriftMapper::mapReport).toList());
        if (hasNextPage) {
            var lastReport = page.getLast();
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

    public String generatePresignedUrl(GeneratePresignedUrlRequest request) throws InvalidRequest, FileNotFound {
        if (request == null) {
            throw invalidRequest("request is required");
        }
        var auditMetadata = requestAuditMetadataResolver.resolve();
        var createdBy = auditMetadata.email();
        var now = Instant.now();
        var fileData = reportQueryDao.getDownloadableFile(createdBy, request.getFileId(), now);
        if (fileData.isEmpty()) {
            throw new FileNotFound();
        }

        var downloadableFile = fileData.get();
        var effectiveExpiresAt = resolveEffectivePresignedUrlExpiresAt(
                request,
                downloadableFile.reportExpiresAt(),
                now
        );
        var url = fileStorageService.generateDownloadUrl(
                downloadableFile.file().getFileId(),
                effectiveExpiresAt
        );
        reportAuditService.writePresignedUrlGenerated(
                downloadableFile.file().getReportId(),
                createdBy,
                auditMetadata,
                request,
                effectiveExpiresAt,
                downloadableFile.file().getFileId()
        );
        return url;
    }

    private Instant resolveEffectivePresignedUrlExpiresAt(
            GeneratePresignedUrlRequest request,
            Instant reportExpiresAt,
            Instant now
    ) throws InvalidRequest {
        var ttlCap = now.plusSeconds(reportProperties.getPresignedUrlTtlSec());
        var requestedExpiresAt = request.isSetRequestedExpiresAt()
                ? TimestampUtils.parse(request.getRequestedExpiresAt())
                : ttlCap;
        if (!requestedExpiresAt.isAfter(now)) {
            throw invalidRequest("requested_expires_at must be in the future");
        }
        var requestAndConfigCap = requestedExpiresAt.isAfter(ttlCap) ? ttlCap : requestedExpiresAt;
        return requestAndConfigCap.isAfter(reportExpiresAt) ? reportExpiresAt : requestAndConfigCap;
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
