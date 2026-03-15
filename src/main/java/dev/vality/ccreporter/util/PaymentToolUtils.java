package dev.vality.ccreporter.util;

import dev.vality.damsel.domain.InvoicePayment;
import lombok.experimental.UtilityClass;

@UtilityClass
public class PaymentToolUtils {

    public static String extractPaymentToolType(InvoicePayment payment) {
        if (payment == null || !payment.isSetPayer()) {
            return null;
        }
        var payer = payment.getPayer();
        if (payer.isSetPaymentResource()
                && payer.getPaymentResource().isSetResource()
                && payer.getPaymentResource().getResource().isSetPaymentTool()
                && payer.getPaymentResource().getResource().getPaymentTool().getSetField() != null) {
            return payer.getPaymentResource().getResource().getPaymentTool().getSetField().getFieldName();
        }
        return payer.getSetField() != null ? payer.getSetField().getFieldName() : null;
    }
}
