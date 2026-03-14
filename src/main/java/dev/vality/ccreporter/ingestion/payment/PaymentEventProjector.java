package dev.vality.ccreporter.ingestion.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.damsel.domain.*;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.damsel.payment_processing.SessionChangePayload;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static dev.vality.ccreporter.util.TimestampUtils.toOptionalLocalDateTime;

@Component
@RequiredArgsConstructor
public class PaymentEventProjector {

    private final ObjectMapper objectMapper;

    public List<PaymentTxnCurrent> project(MachineEvent event, EventPayload payload) {
        var updates = new ArrayList<PaymentTxnCurrent>();
        if (!payload.isSetInvoiceChanges()) {
            return updates;
        }
        for (InvoiceChange change : payload.getInvoiceChanges()) {
            if (change.isSetInvoicePaymentChange()) {
                projectPaymentChange(event, change).ifPresent(updates::add);
            }
        }
        return updates;
    }

    private Optional<PaymentTxnCurrent> projectPaymentChange(MachineEvent event, InvoiceChange change) {
        var paymentChange = change.getInvoicePaymentChange();
        if (paymentChange == null || paymentChange.getPayload() == null) {
            return Optional.empty();
        }

        var context = new PaymentChangeContext(
                event.getSourceId(),
                paymentChange.getId(),
                event.getEventId(),
                Instant.parse(event.getCreatedAt())
        );

        return paymentStartedUpdate(context, paymentChange)
                .or(() -> paymentRouteChangedUpdate(context, paymentChange))
                .or(() -> paymentCashChangedUpdate(context, paymentChange))
                .or(() -> paymentCashFlowChangedUpdate(context, paymentChange))
                .or(() -> paymentStatusChangedUpdate(context, paymentChange))
                .or(() -> paymentTransactionBoundUpdate(context, paymentChange))
                .or(() -> paymentProxyStateFallbackUpdate(context, paymentChange));
    }

    private String paymentToolType(dev.vality.damsel.domain.InvoicePayment payment) {
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

    private Optional<PaymentTxnCurrent> paymentStartedUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentStarted()) {
            return Optional.empty();
        }
        var started = paymentChange.getPayload().getInvoicePaymentStarted();
        var payment = started.getPayment();
        var cost = payment.getCost();
        var route = started.getRoute();
        var status = paymentStatus(payment);
        return Optional.of(baseUpdate(context)
                .setPartyId(payment.isSetPartyRef() ? payment.getPartyRef().getId() : null)
                .setShopId(payment.isSetShopRef() ? payment.getShopRef().getId() : null)
                .setCreatedAt(toOptionalLocalDateTime(Instant.parse(payment.getCreatedAt())))
                .setFinalizedAt(
                        toOptionalLocalDateTime(terminalFinalizedAt(payment.getStatus(), context.eventCreatedAt())))
                .setStatus(status)
                .setProviderId(route != null ? String.valueOf(route.getProvider().getId()) : null)
                .setTerminalId(route != null ? String.valueOf(route.getTerminal().getId()) : null)
                .setAmount(cost.getAmount())
                .setCurrency(cost.getCurrency().getSymbolicCode())
                .setExternalId(payment.getExternalId())
                .setPaymentToolType(paymentToolType(payment)));
    }

    private Optional<PaymentTxnCurrent> paymentRouteChangedUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentRouteChanged()) {
            return Optional.empty();
        }
        var route = paymentChange.getPayload().getInvoicePaymentRouteChanged().getRoute();
        return Optional.of(baseUpdate(context)
                .setProviderId(String.valueOf(route.getProvider().getId()))
                .setTerminalId(String.valueOf(route.getTerminal().getId())));
    }

    private Optional<PaymentTxnCurrent> paymentCashChangedUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentCashChanged()) {
            return Optional.empty();
        }
        var cash = paymentChange.getPayload().getInvoicePaymentCashChanged().getNewCash();
        return Optional.of(baseUpdate(context)
                .setAmount(cash.getAmount())
                .setCurrency(cash.getCurrency().getSymbolicCode())
                .setProviderAmount(cash.getAmount())
                .setProviderCurrency(cash.getCurrency().getSymbolicCode()));
    }

    private Optional<PaymentTxnCurrent> paymentCashFlowChangedUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentCashFlowChanged()) {
            return Optional.empty();
        }
        var postings = paymentChange.getPayload().getInvoicePaymentCashFlowChanged().getCashFlow();
        return Optional.of(baseUpdate(context)
                .setAmount(PaymentCashFlowExtractor.extractAmount(postings)));
    }

    private Optional<PaymentTxnCurrent> paymentStatusChangedUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentStatusChanged()) {
            return Optional.empty();
        }
        var status = paymentChange.getPayload().getInvoicePaymentStatusChanged().getStatus();
        var capturedCost = capturedCost(status);
        return Optional.of(baseUpdate(context)
                .setFinalizedAt(toOptionalLocalDateTime(terminalFinalizedAt(status, context.eventCreatedAt())))
                .setStatus(status.getSetField().getFieldName())
                .setErrorSummary(paymentErrorSummary(status))
                .setProviderAmount(capturedCost != null ? capturedCost.getAmount() : null)
                .setProviderCurrency(symbolicCode(capturedCost)));
    }

    private Optional<PaymentTxnCurrent> paymentTransactionBoundUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        var sessionPayload = sessionPayload(paymentChange);
        if (sessionPayload == null || !sessionPayload.isSetSessionTransactionBound()) {
            return Optional.empty();
        }
        var trx = sessionPayload.getSessionTransactionBound().getTrx();
        var info = trx.getAdditionalInfo();
        return Optional.of(baseUpdate(context)
                .setTrxId(trx.getId())
                .setRrn(info != null ? info.getRrn() : null)
                .setApprovalCode(info != null ? info.getApprovalCode() : null)
                .setConvertedAmount(convertedAmount(trx))
                .setExchangeRateInternal(exchangeRateInternal(trx)));
    }

    private Optional<PaymentTxnCurrent> paymentProxyStateFallbackUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        var sessionPayload = sessionPayload(paymentChange);
        if (sessionPayload == null || !sessionPayload.isSetSessionProxyStateChanged()) {
            return Optional.empty();
        }
        var trxId = providerTrxId(sessionPayload);
        if (trxId == null) {
            return Optional.empty();
        }
        return Optional.of(baseUpdate(context)
                .setTrxId(trxId));
    }

    private PaymentTxnCurrent baseUpdate(PaymentChangeContext context) {
        return new PaymentTxnCurrent()
                .setInvoiceId(context.invoiceId())
                .setPaymentId(context.paymentId())
                .setDomainEventId(context.domainEventId())
                .setDomainEventCreatedAt(toOptionalLocalDateTime(context.eventCreatedAt()));
    }

    private String paymentStatus(dev.vality.damsel.domain.InvoicePayment payment) {
        if (payment == null || !payment.isSetStatus() || payment.getStatus().getSetField() == null) {
            return "pending";
        }
        return payment.getStatus().getSetField().getFieldName();
    }

    private SessionChangePayload sessionPayload(InvoicePaymentChange paymentChange) {
        if (!paymentChange.getPayload().isSetInvoicePaymentSessionChange()) {
            return null;
        }
        return paymentChange.getPayload().getInvoicePaymentSessionChange().getPayload();
    }

    private String providerTrxId(SessionChangePayload payload) {
        var proxyStateChanged = payload.getSessionProxyStateChanged();
        if (proxyStateChanged == null || proxyStateChanged.getProxyState() == null) {
            return null;
        }
        try {
            var proxyStateJson = objectMapper.readTree(new String(
                    proxyStateChanged.getProxyState(),
                    StandardCharsets.UTF_8
            ));
            var providerTrxId = proxyStateJson.path("providerTrxId").asText(null);
            return providerTrxId != null ? providerTrxId : proxyStateJson.path("trxId").asText(null);
        } catch (IOException ex) {
            return null;
        }
    }

    private String paymentErrorSummary(InvoicePaymentStatus status) {
        if (status == null || !status.isSetFailed()) {
            return null;
        }
        return operationFailureSummary(status.getFailed().getFailure());
    }

    private String operationFailureSummary(OperationFailure operationFailure) {
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
        var codes = failureCodes(failure);
        var reason = failure.getReason();
        if (reason == null || reason.isBlank()) {
            return codes;
        }
        return codes == null ? reason : codes + " | " + reason;
    }

    private String failureCodes(Failure failure) {
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

    private Long convertedAmount(TransactionInfo trx) {
        return extraValue(trx, "converted_amount", Long::parseLong);
    }

    private BigDecimal exchangeRateInternal(TransactionInfo trx) {
        return extraValue(trx, "_rate", BigDecimal::new);
    }

    private <T> T extraValue(TransactionInfo trx, String keySuffix, java.util.function.Function<String, T> parser) {
        if (trx == null || trx.getExtra() == null || trx.getExtra().isEmpty()) {
            return null;
        }
        return trx.getExtra().entrySet().stream()
                .filter(entry -> matchesExtraKey(entry.getKey(), keySuffix))
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .map(Map.Entry::getValue)
                .map(value -> parseExtraValue(value, parser))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElse(null);
    }

    private boolean matchesExtraKey(String key, String keySuffix) {
        return key != null && (key.equals(keySuffix) || key.endsWith(keySuffix) || key.contains(keySuffix));
    }

    private <T> Optional<T> parseExtraValue(String value, java.util.function.Function<String, T> parser) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(parser.apply(value));
        } catch (RuntimeException ex) {
            return Optional.empty();
        }
    }

    private Cash capturedCost(InvoicePaymentStatus status) {
        if (status == null || !status.isSetCaptured()) {
            return null;
        }
        return status.getCaptured().getCost();
    }

    private String symbolicCode(Cash cash) {
        return cash != null && cash.isSetCurrency() ? cash.getCurrency().getSymbolicCode() : null;
    }

    private Instant terminalFinalizedAt(InvoicePaymentStatus status, Instant eventCreatedAt) {
        if (status == null || status.getSetField() == null) {
            return null;
        }
        return switch (status.getSetField()) {
            case CAPTURED, CANCELLED, FAILED, REFUNDED, CHARGED_BACK -> eventCreatedAt;
            default -> null;
        };
    }

    private record PaymentChangeContext(
            String invoiceId,
            String paymentId,
            long domainEventId,
            Instant eventCreatedAt
    ) {
    }
}
