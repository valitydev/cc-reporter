package dev.vality.ccreporter.ingestion.withdrawal;

import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import dev.vality.ccreporter.util.TimestampUtils;

import java.math.BigDecimal;
import java.time.Instant;

public final class WithdrawalCurrentUpdateBuilder {

    private final WithdrawalTxnCurrent update;

    private WithdrawalCurrentUpdateBuilder(String withdrawalId, long domainEventId, Instant eventCreatedAt) {
        update = new WithdrawalTxnCurrent();
        update.setWithdrawalId(withdrawalId);
        update.setDomainEventId(domainEventId);
        update.setDomainEventCreatedAt(toLocalDateTime(eventCreatedAt));
    }

    public static WithdrawalCurrentUpdateBuilder builder(
            String withdrawalId,
            long domainEventId,
            Instant eventCreatedAt
    ) {
        return new WithdrawalCurrentUpdateBuilder(withdrawalId, domainEventId, eventCreatedAt);
    }

    public WithdrawalCurrentUpdateBuilder partyId(String partyId) {
        update.setPartyId(partyId);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder walletId(String walletId) {
        update.setWalletId(walletId);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder destinationId(String destinationId) {
        update.setDestinationId(destinationId);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder createdAt(Instant createdAt) {
        update.setCreatedAt(toLocalDateTime(createdAt));
        return this;
    }

    public WithdrawalCurrentUpdateBuilder finalizedAt(Instant finalizedAt) {
        update.setFinalizedAt(toLocalDateTime(finalizedAt));
        return this;
    }

    public WithdrawalCurrentUpdateBuilder status(String status) {
        update.setStatus(status);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder providerId(String providerId) {
        update.setProviderId(providerId);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder terminalId(String terminalId) {
        update.setTerminalId(terminalId);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder amount(Long amount) {
        update.setAmount(amount);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder fee(Long fee) {
        update.setFee(fee);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder currency(String currency) {
        update.setCurrency(currency);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder trxId(String trxId) {
        update.setTrxId(trxId);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder externalId(String externalId) {
        update.setExternalId(externalId);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder errorCode(String errorCode) {
        update.setErrorCode(errorCode);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder errorReason(String errorReason) {
        update.setErrorReason(errorReason);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder errorSubFailure(String errorSubFailure) {
        update.setErrorSubFailure(errorSubFailure);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder originalAmount(Long originalAmount) {
        update.setOriginalAmount(originalAmount);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder originalCurrency(String originalCurrency) {
        update.setOriginalCurrency(originalCurrency);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder convertedAmount(Long convertedAmount) {
        update.setConvertedAmount(convertedAmount);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder exchangeRateInternal(BigDecimal exchangeRateInternal) {
        update.setExchangeRateInternal(exchangeRateInternal);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder providerAmount(Long providerAmount) {
        update.setProviderAmount(providerAmount);
        return this;
    }

    public WithdrawalCurrentUpdateBuilder providerCurrency(String providerCurrency) {
        update.setProviderCurrency(providerCurrency);
        return this;
    }

    public WithdrawalTxnCurrent build() {
        return update;
    }

    private static java.time.LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
    }
}
