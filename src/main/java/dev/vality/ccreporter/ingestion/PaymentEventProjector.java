package dev.vality.ccreporter.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.ccreporter.util.TimestampUtils;
import dev.vality.damsel.domain.Cash;
import dev.vality.damsel.domain.Failure;
import dev.vality.damsel.domain.InvoicePaymentStatus;
import dev.vality.damsel.domain.OperationFailure;
import dev.vality.damsel.domain.TransactionInfo;
import dev.vality.damsel.payment_processing.EventPayload;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class PaymentEventProjector {

    private final ObjectMapper objectMapper;

    public PaymentEventProjector(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

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
                    null,
                    cost.getAmount(),
                    cost.getCurrency().getSymbolicCode(),
                    null,
                    null,
                    null,
                    null
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentRouteChanged()) {
            var route = paymentChange.getPayload().getInvoicePaymentRouteChanged().getRoute();
            return Optional.of(paymentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, String.valueOf(route.getProvider().getId()), null,
                    String.valueOf(route.getTerminal().getId()), null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null, null, null
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentCashChanged()) {
            var cash = paymentChange.getPayload().getInvoicePaymentCashChanged().getNewCash();
            return Optional.of(paymentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, null, null, null, null,
                    cash.getAmount(), null, cash.getCurrency().getSymbolicCode(), null, null, null, null, null,
                    null,
                    cash.getAmount(),
                    cash.getCurrency().getSymbolicCode(),
                    null,
                    null,
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
                    null, null, null, null, null, null, null
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentStatusChanged()) {
            var status = paymentChange.getPayload().getInvoicePaymentStatusChanged().getStatus();
            var capturedCost = capturedCost(status);
            return Optional.of(paymentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, terminalFinalizedAt(status, eventCreatedAt), status.getSetField().getFieldName(),
                    null, null, null, null, null, null, null, null, null, null, null,
                    null,
                    paymentErrorSummary(status),
                    null,
                    null,
                    null,
                    null,
                    capturedCost != null ? capturedCost.getAmount() : null,
                    symbolicCode(capturedCost)
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
                    null,
                    null,
                    null,
                    null,
                    convertedAmount(trx),
                    exchangeRateInternal(trx),
                    null,
                    null
            ));
        }

        if (paymentChange.getPayload().isSetInvoicePaymentSessionChange()
                && paymentChange.getPayload().getInvoicePaymentSessionChange().getPayload()
                .isSetSessionProxyStateChanged()) {
            var trxId = providerTrxId(paymentChange.getPayload().getInvoicePaymentSessionChange().getPayload());
            if (trxId == null) {
                return Optional.empty();
            }
            return Optional.of(paymentUpdate(
                    invoiceId, paymentId, event.getEventId(), eventCreatedAt, null, null, null,
                    null, null, null, null, null, null, null, null, null, null,
                    trxId, null, null, null, null, null, null, null, null, null, null, null
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
            String errorSummary,
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
        update.setErrorSummary(errorSummary);
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

    private String providerTrxId(dev.vality.damsel.payment_processing.SessionChangePayload payload) {
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
}
