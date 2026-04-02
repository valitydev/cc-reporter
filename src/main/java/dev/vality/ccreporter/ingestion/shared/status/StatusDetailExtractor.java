package dev.vality.ccreporter.ingestion.shared.status;

import dev.vality.damsel.domain.Cash;
import dev.vality.damsel.domain.InvoicePaymentStatus;
import dev.vality.fistful.withdrawal.status.Status;
import lombok.experimental.UtilityClass;

import static dev.vality.ccreporter.ingestion.shared.status.FailureSummaryExtractor.summary;

@UtilityClass
public class StatusDetailExtractor {

    public static final String PENDING_STATUS = "pending";

    public static String extractErrorSummary(InvoicePaymentStatus status) {
        if (status == null || !status.isSetFailed()) {
            return null;
        }
        return summary(status.getFailed().getFailure());
    }

    public static String extractErrorSummary(Status status) {
        if (status == null || !status.isSetFailed()) {
            return null;
        }
        return summary(status.getFailed().getFailure());
    }

    public static Cash extractCapturedCost(InvoicePaymentStatus status) {
        if (status == null || !status.isSetCaptured()) {
            return null;
        }
        return status.getCaptured().getCost();
    }

    public static String extractSymbolicCode(Cash cash) {
        return cash != null && cash.isSetCurrency() ? cash.getCurrency().getSymbolicCode() : null;
    }
}
