package dev.vality.ccreporter.report;

import dev.vality.ccreporter.*;
import dev.vality.ccreporter.config.properties.CcrApiProperties;
import dev.vality.ccreporter.config.properties.ReportProperties;
import dev.vality.ccreporter.dao.ReportAuditDao;
import dev.vality.ccreporter.dao.ReportDao;
import dev.vality.ccreporter.security.CurrentPrincipalResolver;
import dev.vality.ccreporter.security.RequestAuditMetadata;
import dev.vality.ccreporter.security.RequestAuditMetadataResolver;
import dev.vality.ccreporter.storage.FileStorageService;
import dev.vality.ccreporter.storage.StoredFileData;
import dev.vality.ccreporter.util.ContinuationTokenCodec;
import dev.vality.ccreporter.util.ThriftQueryCodec;
import dev.vality.ccreporter.util.TimestampUtils;
import org.apache.thrift.TException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;

import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class ReportManagementService {

    private static final String REPORT_CREATED_EVENT = "report_created";
    private static final String REPORT_CANCELED_EVENT = "report_canceled";
    private static final String PRESIGNED_URL_GENERATED_EVENT = "presigned_url_generated";

    private final ReportDao reportDao;
    private final ReportAuditDao reportAuditDao;
    private final ThriftQueryCodec thriftQueryCodec;
    private final ContinuationTokenCodec continuationTokenCodec;
    private final CurrentPrincipalResolver currentPrincipalResolver;
    private final RequestAuditMetadataResolver requestAuditMetadataResolver;
    private final CcrApiProperties apiProperties;
    private final ReportProperties reportProperties;
    private final FileStorageService fileStorageService;
    private final TransactionTemplate transactionTemplate;

    public ReportManagementService(
            ReportDao reportDao,
            ReportAuditDao reportAuditDao,
            ThriftQueryCodec thriftQueryCodec,
            ContinuationTokenCodec continuationTokenCodec,
            CurrentPrincipalResolver currentPrincipalResolver,
            RequestAuditMetadataResolver requestAuditMetadataResolver,
            CcrApiProperties apiProperties,
            ReportProperties reportProperties,
            FileStorageService fileStorageService,
            PlatformTransactionManager transactionManager
    ) {
        this.reportDao = reportDao;
        this.reportAuditDao = reportAuditDao;
        this.thriftQueryCodec = thriftQueryCodec;
        this.continuationTokenCodec = continuationTokenCodec;
        this.currentPrincipalResolver = currentPrincipalResolver;
        this.requestAuditMetadataResolver = requestAuditMetadataResolver;
        this.apiProperties = apiProperties;
        this.reportProperties = reportProperties;
        this.fileStorageService = fileStorageService;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public long createReport(CreateReportRequest request) throws InvalidRequest {
        validateCreateRequest(request);
        var createdBy = currentPrincipalResolver.resolveRequired();
        var auditMetadata = requestAuditMetadataResolver.resolve();
        var timezone = StringUtils.hasText(request.getTimezone()) ? request.getTimezone() : "UTC";
        try {
            var reportId = reportDao.createReport(
                    createdBy,
                    request.getReportType(),
                    request.getFileType(),
                    request.getQuery(),
                    timezone,
                    request.getIdempotencyKey()
            );
            writeCreateAuditEvent(reportId, createdBy, auditMetadata, request, timezone, false);
            return reportId;
        } catch (DuplicateKeyException ex) {
            var reportId = reportDao.findByIdempotencyKey(createdBy, request.getIdempotencyKey())
                    .orElseThrow(() -> ex);
            writeCreateAuditEvent(reportId, createdBy, auditMetadata, request, timezone, true);
            return reportId;
        }
    }

    public Report getReport(GetReportRequest request) throws InvalidRequest, ReportNotFound {
        if (request == null || !request.isSetReportId()) {
            throw invalidRequest("report_id is required");
        }
        var createdBy = currentPrincipalResolver.resolveRequired();
        return reportDao.getReport(createdBy, request.getReportId())
                .map(this::toThriftReport)
                .orElseThrow(ReportNotFound::new);
    }

    public GetReportsResponse getReports(GetReportsRequest request) throws InvalidRequest, BadContinuationToken {
        var createdBy = currentPrincipalResolver.resolveRequired();
        var safeRequest = request == null ? new GetReportsRequest() : request;
        validateGetReportsRequest(safeRequest);

        var meta = safeRequest.getMeta();
        var limit = resolveLimit(meta);
        var cursor = meta != null && meta.isSetContinuationToken()
                ? continuationTokenCodec.decode(meta.getContinuationToken())
                : null;
        var storedReports = reportDao.getReports(createdBy, safeRequest.getFilter(), cursor, limit);

        var response = new GetReportsResponse();
        response.setReports(storedReports.stream().map(this::toThriftReport).toList());
        if (storedReports.size() == limit) {
            var lastReport = storedReports.get(storedReports.size() - 1);
            response.setContinuationToken(continuationTokenCodec.encode(lastReport.createdAt(), lastReport.id()));
        }
        return response;
    }

    public void cancelReport(CancelReportRequest request) throws InvalidRequest, ReportNotFound {
        if (request == null || !request.isSetReportId()) {
            throw invalidRequest("report_id is required");
        }
        var createdBy = currentPrincipalResolver.resolveRequired();
        var auditMetadata = requestAuditMetadataResolver.resolve();
        try {
            transactionTemplate.executeWithoutResult(status -> {
                var updated = reportDao.cancelReport(createdBy, request.getReportId(), Instant.now());
                if (!updated && !reportDao.reportExists(createdBy, request.getReportId())) {
                    throw new ReportNotFoundRuntimeException();
                }
                reportAuditDao.insertEvent(
                        request.getReportId(),
                        REPORT_CANCELED_EVENT,
                        createdBy,
                        auditMetadata,
                        Map.of("state_changed", updated)
                );
            });
        } catch (ReportNotFoundRuntimeException ex) {
            throw new ReportNotFound();
        }
    }

    public String generatePresignedUrl(GeneratePresignedUrlRequest request)
            throws TException {
        if (request == null || !StringUtils.hasText(request.getFileId())) {
            throw invalidRequest("file_id is required");
        }
        var createdBy = currentPrincipalResolver.resolveRequired();
        var auditMetadata = requestAuditMetadataResolver.resolve();
        var fileData = reportDao.getFile(createdBy, request.getFileId());
        if (fileData.isEmpty()) {
            throw new FileNotFound();
        }

        var now = Instant.now();
        var ttlCap = now.plusSeconds(reportProperties.getPresignedUrlTtlSec());
        var requestedExpiresAt = request.isSetRequestedExpiresAt()
                ? TimestampUtils.parse(request.getRequestedExpiresAt())
                : ttlCap;
        if (!requestedExpiresAt.isAfter(now)) {
            throw invalidRequest("requested_expires_at must be in the future");
        }
        var effectiveExpiresAt = requestedExpiresAt.isAfter(ttlCap) ? ttlCap : requestedExpiresAt;
        try {
            var url = fileStorageService.generateDownloadUrl(
                    fileData.get().fileId(),
                    effectiveExpiresAt
            );
            reportAuditDao.insertEvent(
                    fileData.get().reportId(),
                    PRESIGNED_URL_GENERATED_EVENT,
                    createdBy,
                    auditMetadata,
                    buildPresignedUrlDetails(request, effectiveExpiresAt, fileData.get().fileId())
            );
            return url;
        } catch (NoSuchFileException ex) {
            throw new FileNotFound();
        }
    }

    private void writeCreateAuditEvent(
            long reportId,
            String createdBy,
            RequestAuditMetadata auditMetadata,
            CreateReportRequest request,
            String timezone,
            boolean idempotentReplay
    ) {
        reportAuditDao.insertEvent(
                reportId,
                REPORT_CREATED_EVENT,
                createdBy,
                auditMetadata,
                buildCreateDetails(request, timezone, idempotentReplay)
        );
    }

    private Map<String, Object> buildCreateDetails(
            CreateReportRequest request,
            String timezone,
            boolean idempotentReplay
    ) {
        var details = new java.util.LinkedHashMap<String, Object>();
        details.put("reportType", request.getReportType().name());
        details.put("fileType", request.getFileType().name());
        details.put("idempotencyKey", request.getIdempotencyKey());
        details.put("idempotentReplay", idempotentReplay);
        details.put("timezone", timezone);
        return details;
    }

    private Map<String, Object> buildPresignedUrlDetails(
            GeneratePresignedUrlRequest request,
            Instant effectiveExpiresAt,
            String fileId
    ) {
        var details = new java.util.LinkedHashMap<String, Object>();
        details.put("fileId", fileId);
        details.put("requestedExpiresAt", request.isSetRequestedExpiresAt() ? request.getRequestedExpiresAt() : null);
        details.put("effectiveExpiresAt", effectiveExpiresAt.toString());
        return details;
    }

    private static final class ReportNotFoundRuntimeException extends RuntimeException {
    }

    private void validateCreateRequest(CreateReportRequest request) throws InvalidRequest {
        var errors = new ArrayList<String>();
        if (request == null) {
            errors.add("request is required");
        } else {
            if (!request.isSetReportType()) {
                errors.add("report_type is required");
            }
            if (!request.isSetFileType()) {
                errors.add("file_type is required");
            }
            if (!request.isSetQuery()) {
                errors.add("query is required");
            } else {
                validateQuery(request, errors);
            }
            if (StringUtils.hasText(request.getTimezone())) {
                try {
                    Objects.requireNonNull(ZoneId.of(request.getTimezone()));
                } catch (Exception ex) {
                    errors.add("timezone must be a valid IANA timezone");
                }
            }
        }
        if (!errors.isEmpty()) {
            throw new InvalidRequest(errors);
        }
    }

    private void validateQuery(CreateReportRequest request, List<String> errors) {
        var query = request.getQuery();
        var actualType = thriftQueryCodec.resolveReportType(query);
        if (actualType == null) {
            errors.add("query must select exactly one branch");
            return;
        }
        if (request.isSetReportType() && request.getReportType() != actualType) {
            errors.add("report_type does not match query branch");
        }
        var timeRange = thriftQueryCodec.extractTimeRange(query);
        if (!timeRange.to().isAfter(timeRange.from())) {
            errors.add("time_range.from_time must be before time_range.to_time");
        }
    }

    private void validateGetReportsRequest(GetReportsRequest request) throws InvalidRequest {
        var errors = new ArrayList<String>();
        var meta = request.getMeta();
        if (meta != null && meta.isSetLimit() && meta.getLimit() <= 0) {
            errors.add("meta.limit must be positive");
        }
        var filter = request.getFilter();
        if (filter != null && filter.isSetCreatedFrom() && filter.isSetCreatedTo()) {
            var createdFrom = TimestampUtils.parse(filter.getCreatedFrom());
            var createdTo = TimestampUtils.parse(filter.getCreatedTo());
            if (createdFrom.isAfter(createdTo)) {
                errors.add("filter.created_from must be before or equal to filter.created_to");
            }
        }
        if (!errors.isEmpty()) {
            throw new InvalidRequest(errors);
        }
    }

    private int resolveLimit(GetReportsMeta meta) {
        if (meta == null || !meta.isSetLimit()) {
            return apiProperties.getDefaultPageSize();
        }
        return Math.min(meta.getLimit(), apiProperties.getMaxPageSize());
    }

    private Report toThriftReport(StoredReport storedReport) {
        var report = new Report();
        report.setReportId(storedReport.id());
        report.setReportType(storedReport.reportType());
        report.setFileType(storedReport.fileType());
        report.setQuery(thriftQueryCodec.deserialize(storedReport.queryJson()));
        report.setCreatedAt(TimestampUtils.format(storedReport.createdAt()));
        report.setStatus(storedReport.status());
        if (storedReport.startedAt() != null) {
            report.setStartedAt(TimestampUtils.format(storedReport.startedAt()));
        }
        if (storedReport.dataSnapshotFixedAt() != null) {
            report.setDataSnapshotFixedAt(TimestampUtils.format(storedReport.dataSnapshotFixedAt()));
        }
        if (storedReport.finishedAt() != null) {
            report.setFinishedAt(TimestampUtils.format(storedReport.finishedAt()));
        }
        if (storedReport.rowsCount() != null) {
            report.setRowsCount(storedReport.rowsCount());
        }
        if (storedReport.expiresAt() != null) {
            report.setExpiresAt(TimestampUtils.format(storedReport.expiresAt()));
        }
        if (StringUtils.hasText(storedReport.errorCode()) || StringUtils.hasText(storedReport.errorMessage())) {
            report.setError(new ErrorInfo(
                    defaultString(storedReport.errorCode()),
                    defaultString(storedReport.errorMessage())
            ));
        }
        if (storedReport.fileData() != null) {
            report.setFile(toThriftFile(storedReport.fileData()));
        }
        return report;
    }

    private FileMeta toThriftFile(StoredFileData fileData) {
        var fileMeta = new FileMeta();
        fileMeta.setFileId(fileData.fileId());
        fileMeta.setFileType(fileData.fileType());
        fileMeta.setFilename(fileData.filename());
        fileMeta.setContentType(fileData.contentType());
        fileMeta.setSignature(new FileSignature(fileData.md5(), fileData.sha256()));
        if (fileData.sizeBytes() != null) {
            fileMeta.setSizeBytes(fileData.sizeBytes());
        }
        fileMeta.setCreatedAt(TimestampUtils.format(fileData.createdAt()));
        return fileMeta;
    }

    private InvalidRequest invalidRequest(String error) {
        return new InvalidRequest(List.of(error));
    }

    private String defaultString(String value) {
        return value == null ? "" : value;
    }
}
