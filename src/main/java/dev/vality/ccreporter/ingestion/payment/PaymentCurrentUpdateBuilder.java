package dev.vality.ccreporter.ingestion.payment;

import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.ccreporter.util.TimestampUtils;

import java.math.BigDecimal;
import java.time.Instant;

final class PaymentCurrentUpdateBuilder {

    private final PaymentTxnCurrent update;

    private PaymentCurrentUpdateBuilder(
            String invoiceId,
            String paymentId,
            long domainEventId,
            Instant eventCreatedAt
    ) {
        update = new PaymentTxnCurrent();
        update.setInvoiceId(invoiceId);
        update.setPaymentId(paymentId);
        update.setDomainEventId(domainEventId);
        update.setDomainEventCreatedAt(toLocalDateTime(eventCreatedAt));
    }

    static PaymentCurrentUpdateBuilder builder(
            String invoiceId,
            String paymentId,
            long domainEventId,
            Instant eventCreatedAt
    ) {
        return new PaymentCurrentUpdateBuilder(invoiceId, paymentId, domainEventId, eventCreatedAt);
    }

    PaymentCurrentUpdateBuilder partyId(String partyId) {
        update.setPartyId(partyId);
        return this;
    }

    PaymentCurrentUpdateBuilder shopId(String shopId) {
        update.setShopId(shopId);
        return this;
    }

    PaymentCurrentUpdateBuilder createdAt(Instant createdAt) {
        update.setCreatedAt(toLocalDateTime(createdAt));
        return this;
    }

    PaymentCurrentUpdateBuilder finalizedAt(Instant finalizedAt) {
        update.setFinalizedAt(toLocalDateTime(finalizedAt));
        return this;
    }

    PaymentCurrentUpdateBuilder status(String status) {
        update.setStatus(status);
        return this;
    }

    PaymentCurrentUpdateBuilder providerId(String providerId) {
        update.setProviderId(providerId);
        return this;
    }

    PaymentCurrentUpdateBuilder terminalId(String terminalId) {
        update.setTerminalId(terminalId);
        return this;
    }

    PaymentCurrentUpdateBuilder amount(Long amount) {
        update.setAmount(amount);
        return this;
    }

    PaymentCurrentUpdateBuilder fee(Long fee) {
        update.setFee(fee);
        return this;
    }

    PaymentCurrentUpdateBuilder currency(String currency) {
        update.setCurrency(currency);
        return this;
    }

    PaymentCurrentUpdateBuilder trxId(String trxId) {
        update.setTrxId(trxId);
        return this;
    }

    PaymentCurrentUpdateBuilder externalId(String externalId) {
        update.setExternalId(externalId);
        return this;
    }

    PaymentCurrentUpdateBuilder rrn(String rrn) {
        update.setRrn(rrn);
        return this;
    }

    PaymentCurrentUpdateBuilder approvalCode(String approvalCode) {
        update.setApprovalCode(approvalCode);
        return this;
    }

    PaymentCurrentUpdateBuilder paymentToolType(String paymentToolType) {
        update.setPaymentToolType(paymentToolType);
        return this;
    }

    PaymentCurrentUpdateBuilder errorSummary(String errorSummary) {
        update.setErrorSummary(errorSummary);
        return this;
    }

    PaymentCurrentUpdateBuilder originalAmount(Long originalAmount) {
        update.setOriginalAmount(originalAmount);
        return this;
    }

    PaymentCurrentUpdateBuilder originalCurrency(String originalCurrency) {
        update.setOriginalCurrency(originalCurrency);
        return this;
    }

    PaymentCurrentUpdateBuilder convertedAmount(Long convertedAmount) {
        update.setConvertedAmount(convertedAmount);
        return this;
    }

    PaymentCurrentUpdateBuilder exchangeRateInternal(BigDecimal exchangeRateInternal) {
        update.setExchangeRateInternal(exchangeRateInternal);
        return this;
    }

    PaymentCurrentUpdateBuilder providerAmount(Long providerAmount) {
        update.setProviderAmount(providerAmount);
        return this;
    }

    PaymentCurrentUpdateBuilder providerCurrency(String providerCurrency) {
        update.setProviderCurrency(providerCurrency);
        return this;
    }

    PaymentTxnCurrent build() {
        return update;
    }

    private static java.time.LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
    }
}
