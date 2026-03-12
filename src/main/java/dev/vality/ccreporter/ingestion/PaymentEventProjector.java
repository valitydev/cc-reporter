package dev.vality.ccreporter.ingestion;

import dev.vality.damsel.domain.*;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class PaymentEventProjector {

    public List<PaymentCurrentUpdate> project(MachineEvent event, EventPayload payload) {
        var updates = new ArrayList<PaymentCurrentUpdate>();
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

    private Optional<PaymentCurrentUpdate> projectPaymentChange(MachineEvent event, InvoiceChange change) {
        var paymentChange = change.getInvoicePaymentChange();
        if (paymentChange == null || paymentChange.getPayload() == null) {
            return Optional.empty();
        }

        var eventCreatedAt = Instant.parse(event.getCreatedAt());
        var invoiceId = event.getSourceId();
        var paymentId = paymentChange.getId();

        if (paymentChange.getPayload().isSetInvoicePaymentStarted()) {
            var started = paymentChange.getPayload().getInvoicePaymentStarted();
            var payment = started.getPayment();
            var cost = payment.getCost();
            var route = started.getRoute();
            var paymentToolType = payment.isSetPayer() ? payment.getPayer().getSetField().getFieldName() : null;
            var status = payment.isSetStatus() && payment.getStatus().getSetField() != null
                    ? payment.getStatus().getSetField().getFieldName()
                    : "pending";
            return Optional.of(new PaymentCurrentUpdate(
                    invoiceId,
                    paymentId,
                    event.getEventId(),
                    eventCreatedAt,
                    payment.isSetPartyRef() ? payment.getPartyRef().getId() : null,
                    payment.isSetShopRef() ? payment.getShopRef().getId() : null,
                    null, // TODO CCR-INGESTION: confirm event-native source for shop_name/current-state display names.
                    Instant.parse(payment.getCreatedAt()),
                    terminalFinalizedAt(payment.getStatus(), eventCreatedAt),
                    status,
                    route != null ? String.valueOf(route.getProvider().getId()) : null,
                    null, // TODO CCR-INGESTION: confirm event-native source for provider_name in current-state.
                    route != null ? String.valueOf(route.getTerminal().getId()) : null,
                    null, // TODO CCR-INGESTION: confirm event-native source for terminal_name in current-state.
                    cost.getAmount(),
                    null,
                    cost.getCurrency().getSymbolicCode(),
                    null,
                    payment.getExternalId(),
                    null,
                    null,
                    paymentToolType,
                    cost.getAmount(),
                    cost.getCurrency().getSymbolicCode(),
                    cost.getAmount(),
                    BigDecimal.ONE, // TODO CCR-INGESTION: replace mock payments FX mapping with confirmed source.
                    cost.getAmount(), // TODO CCR-INGESTION: replace mock payments FX mapping with confirmed source.
                    cost.getCurrency().getSymbolicCode() // TODO CCR-INGESTION: replace mock payments FX mapping.
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentRouteChanged()) {
            var route = paymentChange.getPayload().getInvoicePaymentRouteChanged().getRoute();
            return Optional.of(new PaymentCurrentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null,
                    null, // TODO CCR-INGESTION: route change still does not populate display names.
                    null, null, null,
                    String.valueOf(route.getProvider().getId()),
                    null, // TODO CCR-INGESTION: confirm provider_name source for route updates.
                    String.valueOf(route.getTerminal().getId()),
                    null, // TODO CCR-INGESTION: confirm terminal_name source for route updates.
                    null, null, null, null, null, null, null, null, null, null, null, null, null, null
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentCashChanged()) {
            var cash = paymentChange.getPayload().getInvoicePaymentCashChanged().getNewCash();
            return Optional.of(new PaymentCurrentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, null, null, null, null,
                    cash.getAmount(), null, cash.getCurrency().getSymbolicCode(), null, null, null, null, null,
                    cash.getAmount(), cash.getCurrency().getSymbolicCode(), cash.getAmount(), BigDecimal.ONE,
                    cash.getAmount(), cash.getCurrency().getSymbolicCode()
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentCashFlowChanged()) {
            var postings = paymentChange.getPayload().getInvoicePaymentCashFlowChanged().getCashFlow();
            return Optional.of(new PaymentCurrentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, null, null, null, null,
                    PaymentCashFlowExtractor.extractAmount(postings),
                    PaymentCashFlowExtractor.extractFee(postings),
                    null, null, null, null, null, null,
                    null, null, null, null, null, null
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentStatusChanged()) {
            var status = paymentChange.getPayload().getInvoicePaymentStatusChanged().getStatus();
            return Optional.of(new PaymentCurrentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, terminalFinalizedAt(status, eventCreatedAt), status.getSetField().getFieldName(),
                    null, null, null, null, null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentSessionChange()
                && paymentChange.getPayload().getInvoicePaymentSessionChange().getPayload()
                .isSetSessionTransactionBound()) {
            var trx = paymentChange.getPayload()
                    .getInvoicePaymentSessionChange()
                    .getPayload()
                    .getSessionTransactionBound()
                    .getTrx();
            var info = trx.getAdditionalInfo();
            return Optional.of(new PaymentCurrentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, null, null, null, null, null, null, null,
                    trx.getId(), null, info != null ? info.getRrn() : null,
                    info != null ? info.getApprovalCode() : null,
                    null, null, null, null, null, null, null
            ));
        }

        return Optional.empty();
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
