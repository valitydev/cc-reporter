package dev.vality.ccreporter.report;

import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.GetReportsRequest;
import dev.vality.ccreporter.InvalidRequest;
import dev.vality.ccreporter.util.TimestampUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
@RequiredArgsConstructor
public class ReportRequestValidator {

    private final ReportQueryService reportQueryService;

    public void validateCreate(CreateReportRequest request) throws InvalidRequest {
        var errors = new ArrayList<String>();
        if (request == null) {
            errors.add("request is required");
        } else {
            validateQuery(request, errors);
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

    public void validateGetReports(GetReportsRequest request) throws InvalidRequest {
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

    private void validateQuery(CreateReportRequest request, List<String> errors) {
        ReportQueryService.QuerySpec querySpec;
        try {
            querySpec = reportQueryService.resolveQuerySpec(request.getQuery());
        } catch (IllegalArgumentException ex) {
            errors.add("query must select exactly one branch");
            return;
        }
        if (request.isSetReportType() && request.getReportType() != querySpec.reportType()) {
            errors.add("report_type does not match query branch");
        }
        if (!querySpec.timeRange().to().isAfter(querySpec.timeRange().from())) {
            errors.add("time_range.from_time must be before time_range.to_time");
        }
    }
}
