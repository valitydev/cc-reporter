package dev.vality.ccreporter.service;

import dev.vality.ccreporter.BadContinuationToken;
import dev.vality.ccreporter.CancelReportRequest;
import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.ErrorInfo;
import dev.vality.ccreporter.FileMeta;
import dev.vality.ccreporter.FileNotFound;
import dev.vality.ccreporter.FileSignature;
import dev.vality.ccreporter.GeneratePresignedUrlRequest;
import dev.vality.ccreporter.GetReportRequest;
import dev.vality.ccreporter.GetReportsFilter;
import dev.vality.ccreporter.GetReportsMeta;
import dev.vality.ccreporter.GetReportsRequest;
import dev.vality.ccreporter.GetReportsResponse;
import dev.vality.ccreporter.InvalidRequest;
import dev.vality.ccreporter.Report;
import dev.vality.ccreporter.ReportNotFound;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportStatus;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.config.CcrApiProperties;
import dev.vality.ccreporter.config.ReportProperties;
import dev.vality.ccreporter.dao.ReportDao;
import dev.vality.ccreporter.util.ContinuationTokenCodec;
import dev.vality.ccreporter.util.ContinuationTokenCodec.PageCursor;
import dev.vality.ccreporter.util.ThriftQueryCodec;
import dev.vality.ccreporter.util.TimestampUtils;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.thrift.TException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class ReportManagementService {

    private final ReportDao reportDao;
    private final ThriftQueryCodec thriftQueryCodec;
    private final ContinuationTokenCodec continuationTokenCodec;
    private final CurrentPrincipalResolver currentPrincipalResolver;
    private final CcrApiProperties apiProperties;
    private final ReportProperties reportProperties;
    private final FileStorageClient fileStorageClient;

    public ReportManagementService(
            ReportDao reportDao,
            ThriftQueryCodec thriftQueryCodec,
            ContinuationTokenCodec continuationTokenCodec,
            CurrentPrincipalResolver currentPrincipalResolver,
            CcrApiProperties apiProperties,
            ReportProperties reportProperties,
            FileStorageClient fileStorageClient
    ) {
        this.reportDao = reportDao;
        this.thriftQueryCodec = thriftQueryCodec;
        this.continuationTokenCodec = continuationTokenCodec;
        this.currentPrincipalResolver = currentPrincipalResolver;
        this.apiProperties = apiProperties;
        this.reportProperties = reportProperties;
        this.fileStorageClient = fileStorageClient;
    }

    public long createReport(CreateReportRequest request) throws InvalidRequest {
        String createdBy = currentPrincipalResolver.resolveRequired();
        validateCreateRequest(request);
        String timezone = StringUtils.hasText(request.getTimezone()) ? request.getTimezone() : "UTC";
        try {
            return reportDao.createReport(
                    createdBy,
                    request.getReportType(),
                    request.getFileType(),
                    request.getQuery(),
                    timezone,
                    request.getIdempotencyKey()
            );
        } catch (DuplicateKeyException ex) {
            return reportDao.findByIdempotencyKey(createdBy, request.getIdempotencyKey())
                    .orElseThrow(() -> ex);
        }
    }

    public Report getReport(GetReportRequest request) throws InvalidRequest, ReportNotFound {
        if (request == null || !request.isSetReportId()) {
            throw invalidRequest("report_id is required");
        }
        String createdBy = currentPrincipalResolver.resolveRequired();
        return reportDao.getReport(createdBy, request.getReportId())
                .map(this::toThriftReport)
                .orElseThrow(ReportNotFound::new);
    }

    public GetReportsResponse getReports(GetReportsRequest request) throws InvalidRequest, BadContinuationToken {
        String createdBy = currentPrincipalResolver.resolveRequired();
        GetReportsRequest safeRequest = request == null ? new GetReportsRequest() : request;
        validateGetReportsRequest(safeRequest);

        GetReportsMeta meta = safeRequest.getMeta();
        int limit = resolveLimit(meta);
        PageCursor cursor = meta != null && meta.isSetContinuationToken()
                ? continuationTokenCodec.decode(meta.getContinuationToken())
                : null;
        List<StoredReport> storedReports = reportDao.getReports(createdBy, safeRequest.getFilter(), cursor, limit);

        GetReportsResponse response = new GetReportsResponse();
        response.setReports(storedReports.stream().map(this::toThriftReport).toList());
        if (storedReports.size() == limit) {
            StoredReport lastReport = storedReports.get(storedReports.size() - 1);
            response.setContinuationToken(continuationTokenCodec.encode(lastReport.createdAt(), lastReport.id()));
        }
        return response;
    }

    public void cancelReport(CancelReportRequest request) throws InvalidRequest, ReportNotFound {
        if (request == null || !request.isSetReportId()) {
            throw invalidRequest("report_id is required");
        }
        String createdBy = currentPrincipalResolver.resolveRequired();
        boolean updated = reportDao.cancelReport(createdBy, request.getReportId(), Instant.now());
        if (!updated && !reportDao.reportExists(createdBy, request.getReportId())) {
            throw new ReportNotFound();
        }
    }

    public String generatePresignedUrl(GeneratePresignedUrlRequest request)
            throws InvalidRequest, FileNotFound, TException {
        if (request == null || !StringUtils.hasText(request.getFileId())) {
            throw invalidRequest("file_id is required");
        }
        String createdBy = currentPrincipalResolver.resolveRequired();
        Optional<StoredFileData> fileData = reportDao.getFile(createdBy, request.getFileId());
        if (fileData.isEmpty()) {
            throw new FileNotFound();
        }

        Instant now = Instant.now();
        Instant ttlCap = now.plusSeconds(reportProperties.getPresignedUrlTtlSec());
        Instant requestedExpiresAt = request.isSetRequestedExpiresAt()
                ? TimestampUtils.parse(request.getRequestedExpiresAt())
                : ttlCap;
        if (!requestedExpiresAt.isAfter(now)) {
            throw invalidRequest("requested_expires_at must be in the future");
        }
        Instant effectiveExpiresAt = requestedExpiresAt.isAfter(ttlCap) ? ttlCap : requestedExpiresAt;
        try {
            return fileStorageClient.generateDownloadUrl(fileData.get().fileId(), effectiveExpiresAt);
        } catch (StoredFileNotFoundException ex) {
            throw new FileNotFound();
        }
    }

    private void validateCreateRequest(CreateReportRequest request) throws InvalidRequest {
        List<String> errors = new ArrayList<>();
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
                    ZoneId.of(request.getTimezone());
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
        ReportQuery query = request.getQuery();
        ReportType actualType = thriftQueryCodec.resolveReportType(query);
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
        List<String> errors = new ArrayList<>();
        GetReportsMeta meta = request.getMeta();
        if (meta != null && meta.isSetLimit() && meta.getLimit() <= 0) {
            errors.add("meta.limit must be positive");
        }
        GetReportsFilter filter = request.getFilter();
        if (filter != null && filter.isSetCreatedFrom() && filter.isSetCreatedTo()) {
            Instant createdFrom = TimestampUtils.parse(filter.getCreatedFrom());
            Instant createdTo = TimestampUtils.parse(filter.getCreatedTo());
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
        Report report = new Report();
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
        FileMeta fileMeta = new FileMeta();
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
