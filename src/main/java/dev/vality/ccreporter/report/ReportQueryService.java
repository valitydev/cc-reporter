package dev.vality.ccreporter.report;

import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.util.TimestampUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class ReportQueryService {

    public QuerySpec resolveQuerySpec(ReportQuery query) {
        if (query == null) {
            throw new IllegalArgumentException("query is required");
        }
        if (query.isSetPayments()) {
            var paymentsQuery = query.getPayments();
            var timeRange = paymentsQuery.getTimeRange();
            return new QuerySpec(
                    ReportType.payments,
                    new QueryTimeRange(
                            TimestampUtils.parse(timeRange.getFromTime()),
                            TimestampUtils.parse(timeRange.getToTime())
                    )
            );
        }
        var withdrawalsQuery = query.getWithdrawals();
        var timeRange = withdrawalsQuery.getTimeRange();
        return new QuerySpec(
                ReportType.withdrawals,
                new QueryTimeRange(
                        TimestampUtils.parse(timeRange.getFromTime()),
                        TimestampUtils.parse(timeRange.getToTime())
                )
        );
    }

    public String hash(String value) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            var hashBytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            var stringBuilder = new StringBuilder(hashBytes.length * 2);
            for (byte hashByte : hashBytes) {
                stringBuilder.append(String.format("%02x", hashByte));
            }
            return stringBuilder.toString();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to hash report query", ex);
        }
    }

    public record QueryTimeRange(Instant from, Instant to) {
    }

    public record QuerySpec(ReportType reportType, QueryTimeRange timeRange) {
    }
}
