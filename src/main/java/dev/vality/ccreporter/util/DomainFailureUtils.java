package dev.vality.ccreporter.util;

import dev.vality.damsel.domain.Failure;
import dev.vality.damsel.domain.OperationFailure;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DomainFailureUtils {

    public static String summary(OperationFailure operationFailure) {
        if (operationFailure == null) {
            return null;
        }
        if (operationFailure.isSetOperationTimeout()) {
            return "operation_timeout";
        }
        if (!operationFailure.isSetFailure()) {
            return null;
        }
        var failure = operationFailure.getFailure();
        var codes = codes(failure);
        var reason = failure.getReason();
        if (reason == null || reason.isBlank()) {
            return codes;
        }
        return codes == null ? reason : codes + " | " + reason;
    }

    public static String summary(dev.vality.fistful.base.Failure failure) {
        var codes = codes(failure);
        var reason = failure.getReason();
        if (reason == null || reason.isBlank()) {
            return codes;
        }
        return codes == null ? reason : codes + " | " + reason;
    }

    public static String codes(Failure failure) {
        if (failure == null || failure.getCode() == null || failure.getCode().isBlank()) {
            return null;
        }
        var codes = new StringBuilder(failure.getCode());
        var subFailure = failure.getSub();
        while (subFailure != null && subFailure.getCode() != null && !subFailure.getCode().isBlank()) {
            codes.append(':').append(subFailure.getCode());
            subFailure = subFailure.getSub();
        }
        return codes.toString();
    }

    public static String codes(dev.vality.fistful.base.Failure failure) {
        if (failure == null || failure.getCode() == null || failure.getCode().isBlank()) {
            return null;
        }
        var codes = new StringBuilder(failure.getCode());
        var subFailure = failure.getSub();
        while (subFailure != null && subFailure.getCode() != null && !subFailure.getCode().isBlank()) {
            codes.append(':').append(subFailure.getCode());
            subFailure = subFailure.getSub();
        }
        return codes.toString();
    }
}
