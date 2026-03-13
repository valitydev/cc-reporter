package dev.vality.ccreporter.integration.fixture;

import dev.vality.ccreporter.*;

import java.time.Instant;

/**
 * Держит заготовки запросов на отчёт, чтобы тесты читались как сценарии, а не как сборка Thrift-структур по кускам.
 */
public final class ReportRequestFixtures {

    private ReportRequestFixtures() {
    }

    public static CreateReportRequest payments(String idempotencyKey) {
        return payments(idempotencyKey, defaultTimeRange());
    }

    public static CreateReportRequest payments(String idempotencyKey, TimeRange timeRange) {
        var paymentsQuery = new PaymentsQuery();
        paymentsQuery.setTimeRange(timeRange);
        var reportQuery = new ReportQuery();
        reportQuery.setPayments(paymentsQuery);

        var request = new CreateReportRequest();
        request.setReportType(ReportType.payments);
        request.setFileType(FileType.csv);
        request.setQuery(reportQuery);
        request.setIdempotencyKey(idempotencyKey);
        return request;
    }

    public static CreateReportRequest withdrawals(String idempotencyKey) {
        return withdrawals(idempotencyKey, defaultTimeRange());
    }

    public static CreateReportRequest withdrawals(String idempotencyKey, TimeRange timeRange) {
        var withdrawalsQuery = new WithdrawalsQuery();
        withdrawalsQuery.setTimeRange(timeRange);
        var reportQuery = new ReportQuery();
        reportQuery.setWithdrawals(withdrawalsQuery);

        var request = new CreateReportRequest();
        request.setReportType(ReportType.withdrawals);
        request.setFileType(FileType.csv);
        request.setQuery(reportQuery);
        request.setIdempotencyKey(idempotencyKey);
        return request;
    }

    public static TimeRange defaultTimeRange() {
        return timeRange("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z");
    }

    public static TimeRange timeRange(String from, String to) {
        return new TimeRange(
                Instant.parse(from).toString(),
                Instant.parse(to).toString()
        );
    }
}
