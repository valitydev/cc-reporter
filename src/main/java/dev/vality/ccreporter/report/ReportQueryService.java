package dev.vality.ccreporter.report;

import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.WithdrawalsQuery;
import dev.vality.ccreporter.util.TimestampUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class ReportQueryService {

    public ReportType resolveReportType(ReportQuery query) {
        var payments = query != null && query.isSetPayments();
        var withdrawals = query != null && query.isSetWithdrawals();
        if (payments == withdrawals) {
            return null;
        }
        return payments ? ReportType.payments : ReportType.withdrawals;
    }

    public QueryTimeRange extractTimeRange(ReportQuery query) {
        if (query == null) {
            throw new IllegalArgumentException("query is required");
        }
        String fromTime;
        String toTime;
        if (query.isSetPayments()) {
            var paymentsQuery = query.getPayments();
            fromTime = paymentsQuery.getTimeRange().getFromTime();
            toTime = paymentsQuery.getTimeRange().getToTime();
        } else if (query.isSetWithdrawals()) {
            var withdrawalsQuery = query.getWithdrawals();
            fromTime = withdrawalsQuery.getTimeRange().getFromTime();
            toTime = withdrawalsQuery.getTimeRange().getToTime();
        } else {
            throw new IllegalArgumentException("query must select exactly one branch");
        }
        return new QueryTimeRange(
                TimestampUtils.parse(fromTime),
                TimestampUtils.parse(toTime)
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
}
