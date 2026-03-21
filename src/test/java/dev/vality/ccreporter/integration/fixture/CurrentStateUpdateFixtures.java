package dev.vality.ccreporter.integration.fixture;

import dev.vality.ccreporter.domain.tables.pojos.PaymentTxnCurrent;
import dev.vality.ccreporter.domain.tables.pojos.WithdrawalSession;
import dev.vality.ccreporter.domain.tables.pojos.WithdrawalTxnCurrent;
import dev.vality.ccreporter.util.TimestampUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;

import static dev.vality.ccreporter.util.SearchValueNormalizer.normalize;

/**
 * Собирает доменные апдейты для DAO-тестов, чтобы сценарии не расползались из-за ручной сборки всех полей.
 */
public final class CurrentStateUpdateFixtures {

    private CurrentStateUpdateFixtures() {
    }

    public static PaymentTxnCurrent paymentUpdate(long eventId, String status, Instant finalizedAt) {
        return new PaymentTxnCurrent()
                .setInvoiceId("invoice-1")
                .setPaymentId("payment-1")
                .setDomainEventId(eventId)
                .setDomainEventCreatedAt(TimestampUtils.toLocalDateTime(
                        Instant.parse("2026-01-01T10:00:00Z").plusSeconds(eventId)
                ))
                .setPartyId("party-1")
                .setShopId("shop-1")
                .setCreatedAt(toLocalDateTime(Instant.parse("2026-01-01T10:00:00Z")))
                .setFinalizedAt(toLocalDateTime(finalizedAt))
                .setStatus(status)
                .setProviderId("provider-1")
                .setTerminalId("terminal-1")
                .setAmount(1000L)
                .setFee(10L)
                .setCurrency("RUB")
                .setTrxId("trx-1")
                .setExternalId("external-1")
                .setRrn("rrn-1")
                .setApprovalCode("approval-1")
                .setPaymentToolType("bank_card")
                .setOriginalAmount(1000L)
                .setOriginalCurrency("RUB")
                .setConvertedAmount(1000L)
                .setExchangeRateInternal(BigDecimal.ONE)
                .setProviderAmount(1000L)
                .setProviderCurrency("RUB");
    }

    public static WithdrawalTxnCurrent withdrawalUpdate(
            long eventId,
            String status,
            Instant finalizedAt
    ) {
        return new WithdrawalTxnCurrent()
                .setWithdrawalId("withdrawal-1")
                .setDomainEventId(eventId)
                .setDomainEventCreatedAt(TimestampUtils.toLocalDateTime(
                        Instant.parse("2026-01-01T11:00:00Z").plusSeconds(eventId)
                ))
                .setPartyId("party-1")
                .setWalletId("wallet-1")
                .setDestinationId("destination-1")
                .setCreatedAt(toLocalDateTime(Instant.parse("2026-01-01T11:00:00Z")))
                .setFinalizedAt(toLocalDateTime(finalizedAt))
                .setStatus(status)
                .setProviderId("provider-1")
                .setTerminalId("terminal-1")
                .setAmount(2000L)
                .setFee(20L)
                .setCurrency("RUB")
                .setExternalId("external-1")
                .setOriginalAmount(2100L)
                .setOriginalCurrency("USD")
                .setExchangeRateInternal(new BigDecimal("0.9523809524"))
                .setProviderAmount(2000L)
                .setProviderCurrency("RUB");
    }

    public static WithdrawalSession withdrawalSessionUpdate(
            String sessionId,
            String withdrawalId,
            long eventId,
            Instant eventCreatedAt,
            String trxId
    ) {
        var session = new WithdrawalSession()
                .setSessionId(sessionId)
                .setWithdrawalId(withdrawalId)
                .setDomainEventId(eventId)
                .setDomainEventCreatedAt(toLocalDateTime(eventCreatedAt));
        if (trxId != null) {
            session.setTrxId(trxId).setTrxSearch(normalize(trxId));
        }
        return session;
    }

    private static LocalDateTime toLocalDateTime(Instant value) {
        return value == null ? null : TimestampUtils.toLocalDateTime(value);
    }
}
