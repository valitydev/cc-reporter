package dev.vality.ccreporter.ingestion;

import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.ccreporter.util.TimestampUtils;
import dev.vality.damsel.domain.InvoicePaymentStatus;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class PaymentEventProjector {

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

        var eventCreatedAt = Instant.parse(event.getCreatedAt());
        var invoiceId = event.getSourceId();
        var paymentId = paymentChange.getId();

        if (paymentChange.getPayload().isSetInvoicePaymentStarted()) {
            var started = paymentChange.getPayload().getInvoicePaymentStarted();
            var payment = started.getPayment();
            var cost = payment.getCost();
            var route = started.getRoute();
            var paymentToolType = paymentToolType(payment);
            var status = payment.isSetStatus() && payment.getStatus().getSetField() != null
                    ? payment.getStatus().getSetField().getFieldName()
                    : "pending";
            return Optional.of(paymentUpdate(
                    invoiceId,
                    paymentId,
                    event.getEventId(),
                    eventCreatedAt,
                    payment.isSetPartyRef() ? payment.getPartyRef().getId() : null,
                    payment.isSetShopRef() ? payment.getShopRef().getId() : null,
                    null,
                    Instant.parse(payment.getCreatedAt()),
                    terminalFinalizedAt(payment.getStatus(), eventCreatedAt),
                    status,
                    route != null ? String.valueOf(route.getProvider().getId()) : null,
                    null,
                    route != null ? String.valueOf(route.getTerminal().getId()) : null,
                    null,
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
                    BigDecimal.ONE,
                    cost.getAmount(),
                    cost.getCurrency().getSymbolicCode()
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentRouteChanged()) {
            var route = paymentChange.getPayload().getInvoicePaymentRouteChanged().getRoute();
            return Optional.of(paymentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, String.valueOf(route.getProvider().getId()), null,
                    String.valueOf(route.getTerminal().getId()), null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null, null
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentCashChanged()) {
            var cash = paymentChange.getPayload().getInvoicePaymentCashChanged().getNewCash();
            return Optional.of(paymentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, null, null, null, null,
                    cash.getAmount(), null, cash.getCurrency().getSymbolicCode(), null, null, null, null, null,
                    cash.getAmount(), cash.getCurrency().getSymbolicCode(), cash.getAmount(), BigDecimal.ONE,
                    cash.getAmount(), cash.getCurrency().getSymbolicCode()
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentCashFlowChanged()) {
            var postings = paymentChange.getPayload().getInvoicePaymentCashFlowChanged().getCashFlow();
            return Optional.of(paymentUpdate(
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
            return Optional.of(paymentUpdate(
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
            return Optional.of(paymentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, null, null, null, null, null, null, null,
                    trx.getId(), null, info != null ? info.getRrn() : null,
                    info != null ? info.getApprovalCode() : null,
                    null, null, null, null, null, null, null
            ));
        }

        return Optional.empty();
    }

    private PaymentTxnCurrent paymentUpdate(
            String invoiceId,
            String paymentId,
            long domainEventId,
            Instant domainEventCreatedAt,
            String partyId,
            String shopId,
            String shopName,
            Instant createdAt,
            Instant finalizedAt,
            String status,
            String providerId,
            String providerName,
            String terminalId,
            String terminalName,
            Long amount,
            Long fee,
            String currency,
            String trxId,
            String externalId,
            String rrn,
            String approvalCode,
            String paymentToolType,
            Long originalAmount,
            String originalCurrency,
            Long convertedAmount,
            BigDecimal exchangeRateInternal,
            Long providerAmount,
            String providerCurrency
    ) {
        var update = new PaymentTxnCurrent();
        update.setInvoiceId(invoiceId);
        update.setPaymentId(paymentId);
        update.setDomainEventId(domainEventId);
        update.setDomainEventCreatedAt(toLocalDateTime(domainEventCreatedAt));
        update.setPartyId(partyId);
        update.setShopId(shopId);
        update.setShopName(shopName);
        update.setCreatedAt(toLocalDateTime(createdAt));
        update.setFinalizedAt(toLocalDateTime(finalizedAt));
        update.setStatus(status);
        update.setProviderId(providerId);
        update.setProviderName(providerName);
        update.setTerminalId(terminalId);
        update.setTerminalName(terminalName);
        update.setAmount(amount);
        update.setFee(fee);
        update.setCurrency(currency);
        update.setTrxId(trxId);
        update.setExternalId(externalId);
        update.setRrn(rrn);
        update.setApprovalCode(approvalCode);
        update.setPaymentToolType(paymentToolType);
        update.setOriginalAmount(originalAmount);
        update.setOriginalCurrency(originalCurrency);
        update.setConvertedAmount(convertedAmount);
        update.setExchangeRateInternal(exchangeRateInternal);
        update.setProviderAmount(providerAmount);
        update.setProviderCurrency(providerCurrency);
        return update;
    }

    private LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
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
