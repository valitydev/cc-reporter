package dev.vality.ccreporter.integration.fixture;

import dev.vality.ccreporter.CreateReportRequest;
import dev.vality.ccreporter.FileType;
import dev.vality.ccreporter.PaymentsQuery;
import dev.vality.ccreporter.ReportQuery;
import dev.vality.ccreporter.ReportType;
import dev.vality.ccreporter.TimeRange;
import dev.vality.ccreporter.WithdrawalsQuery;

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
        PaymentsQuery paymentsQuery = new PaymentsQuery();
        paymentsQuery.setTimeRange(timeRange);
        ReportQuery reportQuery = new ReportQuery();
        reportQuery.setPayments(paymentsQuery);

        CreateReportRequest request = new CreateReportRequest();
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
        WithdrawalsQuery withdrawalsQuery = new WithdrawalsQuery();
        withdrawalsQuery.setTimeRange(timeRange);
        ReportQuery reportQuery = new ReportQuery();
        reportQuery.setWithdrawals(withdrawalsQuery);

        CreateReportRequest request = new CreateReportRequest();
        request.setReportType(ReportType.withdrawals);
        request.setFileType(FileType.csv);
        request.setQuery(reportQuery);
        request.setIdempotencyKey(idempotencyKey);
        return request;
    }

    public static TimeRange defaultTimeRange() {
        return new TimeRange(
                Instant.parse("2026-01-01T00:00:00Z").toString(),
                Instant.parse("2026-01-02T00:00:00Z").toString()
        );
    }
}
