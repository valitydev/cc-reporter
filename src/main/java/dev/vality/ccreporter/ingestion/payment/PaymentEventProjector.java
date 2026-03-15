package dev.vality.ccreporter.ingestion.payment;

import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.ccreporter.util.DomainCashFlowExtractor;
import dev.vality.ccreporter.util.ProxyStateExtractor;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.damsel.payment_processing.SessionChangePayload;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static dev.vality.ccreporter.util.DomainStatusUtils.*;
import static dev.vality.ccreporter.util.PaymentToolUtils.extractPaymentToolType;
import static dev.vality.ccreporter.util.TimestampUtils.toOptionalLocalDateTime;
import static dev.vality.ccreporter.util.TransactionExtraUtils.getConvertedAmount;
import static dev.vality.ccreporter.util.TransactionExtraUtils.getExchangeRate;

@Component
@RequiredArgsConstructor
public class PaymentEventProjector {

    private final ProxyStateExtractor proxyStateExtractor;

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
        return Optional.of(baseUpdate(context)
                .setPartyId(payment.isSetPartyRef() ? payment.getPartyRef().getId() : null)
                .setShopId(payment.isSetShopRef() ? payment.getShopRef().getId() : null)
                .setCreatedAt(toOptionalLocalDateTime(Instant.parse(payment.getCreatedAt())))
                .setStatus(PENDING_STATUS)
                .setProviderId(route != null ? String.valueOf(route.getProvider().getId()) : null)
                .setTerminalId(route != null ? String.valueOf(route.getTerminal().getId()) : null)
                .setAmount(cost.getAmount())
                .setCurrency(cost.getCurrency().getSymbolicCode())
                .setExternalId(payment.getExternalId())
                .setPaymentToolType(extractPaymentToolType(payment))
                .setOriginalAmount(cost.getAmount())
                .setOriginalCurrency(cost.getCurrency().getSymbolicCode()));
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
                .setCurrency(cash.getCurrency().getSymbolicCode()));
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
                .setAmount(DomainCashFlowExtractor.extractPaymentAmount(postings))
                .setFee(DomainCashFlowExtractor.extractPaymentFee(postings)));
    }

    private Optional<PaymentTxnCurrent> paymentStatusChangedUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentStatusChanged()) {
            return Optional.empty();
        }
        var status = paymentChange.getPayload().getInvoicePaymentStatusChanged().getStatus();
        var capturedCost = extractCapturedCost(status);
        return Optional.of(baseUpdate(context)
                .setFinalizedAt(toOptionalLocalDateTime(resolveTerminalFinalizedAt(status, context.eventCreatedAt())))
                .setStatus(status.getSetField().getFieldName())
                .setErrorSummary(extractErrorSummary(status))
                .setProviderAmount(capturedCost != null ? capturedCost.getAmount() : null)
                .setProviderCurrency(extractSymbolicCode(capturedCost)));
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
                .setConvertedAmount(getConvertedAmount(trx))
                .setExchangeRateInternal(getExchangeRate(trx)));
    }

    private Optional<PaymentTxnCurrent> paymentProxyStateFallbackUpdate(
            PaymentChangeContext context,
            InvoicePaymentChange paymentChange
    ) {
        var sessionPayload = sessionPayload(paymentChange);
        if (sessionPayload == null || !sessionPayload.isSetSessionProxyStateChanged()) {
            return Optional.empty();
        }
        var trxId = proxyStateExtractor.extractProviderTrxId(sessionPayload);
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

    private SessionChangePayload sessionPayload(InvoicePaymentChange paymentChange) {
        if (!paymentChange.getPayload().isSetInvoicePaymentSessionChange()) {
            return null;
        }
        return paymentChange.getPayload().getInvoicePaymentSessionChange().getPayload();
    }

    private record PaymentChangeContext(
            String invoiceId,
            String paymentId,
            long domainEventId,
            Instant eventCreatedAt
    ) {
    }
}
