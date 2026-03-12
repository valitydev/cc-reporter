package dev.vality.ccreporter.ingestion;

import java.math.BigDecimal;
import java.time.Instant;

public record WithdrawalCurrentUpdate(
        String withdrawalId,
        long domainEventId,
        Instant domainEventCreatedAt,
        String partyId,
        String walletId,
        String walletName,
        String destinationId,
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
        String errorCode,
        String errorReason,
        String errorSubFailure,
        Long originalAmount,
        String originalCurrency,
        Long convertedAmount,
        BigDecimal exchangeRateInternal,
        Long providerAmount,
        String providerCurrency
) {
}
