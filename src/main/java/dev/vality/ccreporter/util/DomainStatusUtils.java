package dev.vality.ccreporter.util;

import dev.vality.damsel.domain.Cash;
import dev.vality.damsel.domain.InvoicePaymentStatus;
import dev.vality.fistful.withdrawal.status.Status;
import lombok.experimental.UtilityClass;

import java.time.Instant;

import static dev.vality.ccreporter.util.DomainFailureUtils.summary;

@UtilityClass
public class DomainStatusUtils {

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

    public static Instant resolveTerminalFinalizedAt(InvoicePaymentStatus status, Instant eventCreatedAt) {
        if (status == null || status.getSetField() == null) {
            return null;
        }
        return switch (status.getSetField()) {
            case CAPTURED, CANCELLED, FAILED, REFUNDED, CHARGED_BACK -> eventCreatedAt;
            default -> null;
        };
    }
}
