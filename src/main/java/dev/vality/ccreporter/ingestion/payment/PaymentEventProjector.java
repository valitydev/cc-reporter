package dev.vality.ccreporter.ingestion.payment;

import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.ccreporter.ingestion.payment.support.PaymentToolExtractor;
import dev.vality.ccreporter.ingestion.payment.support.ProxyStateExtractor;
import dev.vality.ccreporter.ingestion.payment.support.TransactionExtraExtractor;
import dev.vality.ccreporter.ingestion.shared.cashflow.CashFlowAmountExtractor;
import dev.vality.damsel.domain.InvoicePaymentStatus;
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

import static dev.vality.ccreporter.ingestion.shared.status.StatusDetailExtractor.*;
import static dev.vality.ccreporter.util.SearchValueNormalizer.normalize;
import static dev.vality.ccreporter.util.TimestampUtils.toLocalDateTime;
import static dev.vality.ccreporter.util.TimestampUtils.toOptionalLocalDateTime;

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

        return paymentStartedUpdate(event, paymentChange)
                .or(() -> paymentRouteChangedUpdate(event, paymentChange))
                .or(() -> paymentCashChangedUpdate(event, paymentChange))
                .or(() -> paymentCashFlowChangedUpdate(event, paymentChange))
                .or(() -> paymentStatusChangedUpdate(event, paymentChange))
                .or(() -> paymentTransactionBoundUpdate(event, paymentChange))
                .or(() -> paymentProxyStateFallbackUpdate(event, paymentChange));
    }

    private Optional<PaymentTxnCurrent> paymentStartedUpdate(
            MachineEvent event,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentStarted()) {
            return Optional.empty();
        }
        var started = paymentChange.getPayload().getInvoicePaymentStarted();
        var payment = started.getPayment();
        var cost = payment.getCost();
        var route = started.getRoute();
        return Optional.of(baseUpdate(event, paymentChange)
                .setPartyId(payment.isSetPartyRef() ? payment.getPartyRef().getId() : null)
                .setShopId(payment.isSetShopRef() ? payment.getShopRef().getId() : null)
                .setCreatedAt(toLocalDateTime(payment.getCreatedAt()))
                .setStatus(PENDING_STATUS)
                .setProviderId(route != null ? String.valueOf(route.getProvider().getId()) : null)
                .setTerminalId(route != null ? String.valueOf(route.getTerminal().getId()) : null)
                .setAmount(cost.getAmount())
                .setCurrency(cost.getCurrency().getSymbolicCode())
                .setExternalId(payment.getExternalId())
                .setPaymentToolType(PaymentToolExtractor.extractPaymentToolType(payment))
                .setOriginalAmount(cost.getAmount())
                .setOriginalCurrency(cost.getCurrency().getSymbolicCode()));
    }

    private Optional<PaymentTxnCurrent> paymentRouteChangedUpdate(
            MachineEvent event,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentRouteChanged()) {
            return Optional.empty();
        }
        var route = paymentChange.getPayload().getInvoicePaymentRouteChanged().getRoute();
        return Optional.of(baseUpdate(event, paymentChange)
                .setProviderId(String.valueOf(route.getProvider().getId()))
                .setTerminalId(String.valueOf(route.getTerminal().getId())));
    }

    private Optional<PaymentTxnCurrent> paymentCashChangedUpdate(
            MachineEvent event,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentCashChanged()) {
            return Optional.empty();
        }
        var cash = paymentChange.getPayload().getInvoicePaymentCashChanged().getNewCash();
        return Optional.of(baseUpdate(event, paymentChange)
                .setAmount(cash.getAmount())
                .setCurrency(cash.getCurrency().getSymbolicCode()));
    }

    private Optional<PaymentTxnCurrent> paymentCashFlowChangedUpdate(
            MachineEvent event,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentCashFlowChanged()) {
            return Optional.empty();
        }
        var postings = paymentChange.getPayload().getInvoicePaymentCashFlowChanged().getCashFlow();
        return Optional.of(baseUpdate(event, paymentChange)
                .setAmount(CashFlowAmountExtractor.extractPaymentAmount(postings))
                .setFee(CashFlowAmountExtractor.extractPaymentFee(postings)));
    }

    private Optional<PaymentTxnCurrent> paymentStatusChangedUpdate(
            MachineEvent event,
            InvoicePaymentChange paymentChange
    ) {
        if (!paymentChange.getPayload().isSetInvoicePaymentStatusChanged()) {
            return Optional.empty();
        }
        var status = paymentChange.getPayload().getInvoicePaymentStatusChanged().getStatus();
        var capturedCost = extractCapturedCost(status);
        return Optional.of(baseUpdate(event, paymentChange)
                .setFinalizedAt(
                        toOptionalLocalDateTime(terminalFinalizedAt(status, Instant.parse(event.getCreatedAt()))))
                .setStatus(status.getSetField().getFieldName())
                .setErrorSummary(extractErrorSummary(status))
                .setProviderAmount(capturedCost != null ? capturedCost.getAmount() : null)
                .setProviderCurrency(extractSymbolicCode(capturedCost)));
    }

    private Optional<PaymentTxnCurrent> paymentTransactionBoundUpdate(
            MachineEvent event,
            InvoicePaymentChange paymentChange
    ) {
        var sessionPayload = sessionPayload(paymentChange);
        if (sessionPayload == null || !sessionPayload.isSetSessionTransactionBound()) {
            return Optional.empty();
        }
        var trx = sessionPayload.getSessionTransactionBound().getTrx();
        var info = trx.getAdditionalInfo();
        var code = info != null ? info.getApprovalCode() : null;
        var rrn = info != null ? info.getRrn() : null;
        return Optional.of(baseUpdate(event, paymentChange)
                .setTrxId(trx.getId())
                .setRrn(rrn)
                .setApprovalCode(code)
                .setConvertedAmount(TransactionExtraExtractor.getConvertedAmount(trx))
                .setExchangeRateInternal(TransactionExtraExtractor.getExchangeRate(trx))
                .setTrxSearch(normalize(trx.getId(), rrn, code)));
    }

    private Optional<PaymentTxnCurrent> paymentProxyStateFallbackUpdate(
            MachineEvent event,
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
        return Optional.of(baseUpdate(event, paymentChange)
                .setTrxId(trxId)
                .setTrxSearch(normalize(trxId, null, null)));
    }

    private PaymentTxnCurrent baseUpdate(MachineEvent event, InvoicePaymentChange paymentChange) {
        return new PaymentTxnCurrent()
                .setInvoiceId(event.getSourceId())
                .setPaymentId(paymentChange.getId())
                .setDomainEventId(event.getEventId())
                .setDomainEventCreatedAt(toLocalDateTime(event.getCreatedAt()));
    }

    private SessionChangePayload sessionPayload(InvoicePaymentChange paymentChange) {
        if (!paymentChange.getPayload().isSetInvoicePaymentSessionChange()) {
            return null;
        }
        return paymentChange.getPayload().getInvoicePaymentSessionChange().getPayload();
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
}
