package dev.vality.ccreporter.ingestion;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCurrentUpdate(
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
}
