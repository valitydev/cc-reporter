package dev.vality.ccreporter.report;

import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.GetReportsRequest;
import dev.vality.ccreporter.InvalidRequest;
import dev.vality.ccreporter.util.TimestampUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class ReportRequestValidator {

    private final ReportQueryService reportQueryService;

    public void validateCreate(CreateReportRequest request) throws InvalidRequest {
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
            validateQuery(request, errors);
            validateTimezone(request.getTimezone(), errors);
        }
        if (!errors.isEmpty()) {
            throw new InvalidRequest(errors);
        }
    }

    public void validateGetReports(GetReportsRequest request) throws InvalidRequest {
        var errors = new ArrayList<String>();
        var meta = request.getMeta();
        if (meta != null && meta.isSetLimit() && meta.getLimit() <= 0) {
            errors.add("meta.limit must be positive");
        }
        var filter = request.getFilter();
        if (filter != null) {
            var createdFrom = parseFilterTimestamp(
                    filter.isSetCreatedFrom(),
                    filter.getCreatedFrom(),
                    "filter.created_from",
                    errors
            );
            var createdTo = parseFilterTimestamp(
                    filter.isSetCreatedTo(),
                    filter.getCreatedTo(),
                    "filter.created_to",
                    errors
            );
            if (createdFrom != null && createdTo != null && createdFrom.isAfter(createdTo)) {
                errors.add("filter.created_from must be before or equal to filter.created_to");
            }
        }
        if (!errors.isEmpty()) {
            throw new InvalidRequest(errors);
        }
    }

    private Instant parseFilterTimestamp(
            boolean isSet,
            String value,
            String fieldName,
            List<String> errors
    ) {
        if (!isSet) {
            return null;
        }
        try {
            return TimestampUtils.parse(value);
        } catch (DateTimeException ex) {
            errors.add(fieldName + " must use ISO-8601 format");
            return null;
        }
    }

    private void validateQuery(CreateReportRequest request, List<String> errors) {
        ReportQueryService.QuerySpec querySpec;
        try {
            querySpec = reportQueryService.resolveQuerySpec(request.getQuery());
        } catch (IllegalArgumentException ex) {
            errors.add(ex.getMessage());
            return;
        }
        if (request.isSetReportType() && request.getReportType() != querySpec.reportType()) {
            errors.add("report_type does not match query branch");
        }
        if (!querySpec.timeRange().to().isAfter(querySpec.timeRange().from())) {
            errors.add("time_range.from_time must be before time_range.to_time");
        }
    }

    private void validateTimezone(String timezone, List<String> errors) {
        if (!StringUtils.hasText(timezone)) {
            return;
        }
        try {
            ZoneId.of(timezone);
        } catch (DateTimeException ex) {
            errors.add("timezone must be a valid IANA timezone");
        }
    }
}
