package dev.vality.ccreporter.report;

import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.TimeRange;
import dev.vality.ccreporter.util.TimestampUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.DateTimeException;
import java.time.Instant;

@Service
public class ReportQueryService {

    public QuerySpec resolveQuerySpec(ReportQuery query) {
        if (query == null) {
            throw new IllegalArgumentException("query is required");
        }
        if (query.isSetPayments()) {
            return new QuerySpec(ReportType.payments, parseTimeRange(query.getPayments().getTimeRange()));
        }
        if (query.isSetWithdrawals()) {
            return new QuerySpec(ReportType.withdrawals, parseTimeRange(query.getWithdrawals().getTimeRange()));
        }
        throw new IllegalArgumentException("query must select one branch");
    }

    private QueryTimeRange parseTimeRange(TimeRange timeRange) {
        if (timeRange == null
                || !StringUtils.hasText(timeRange.getFromTime())
                || !StringUtils.hasText(timeRange.getToTime())) {
            throw new IllegalArgumentException("time range is required");
        }
        try {
            return new QueryTimeRange(
                    TimestampUtils.parse(timeRange.getFromTime()),
                    TimestampUtils.parse(timeRange.getToTime())
            );
        } catch (DateTimeException ex) {
            throw new IllegalArgumentException("time range must contain ISO-8601 timestamps", ex);
        }
    }

    public record QueryTimeRange(Instant from, Instant to) {
    }

    public record QuerySpec(ReportType reportType, QueryTimeRange timeRange) {
    }
}
